#!/usr/bin/env python

import argparse
import contextlib
import csv
import json
import os
import pathlib

import lxml.etree

from .assembly import Assembly
from .manifest import Manifest
from .sample import SampleSet
from .study import Study
from .submission import Submission, SubmissionResponse
from .webin import get_webin_credentials, EnaWebinClient


@contextlib.contextmanager
def working_directory(path):
    # https://stackoverflow.com/questions/41742317/how-can-i-change-directory-with-python-pathlib
    """Changes working directory and returns to previous on exit."""
    prev_cwd = pathlib.Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


def parse_assemblies(f):
    with open(f, "rt", encoding="UTF-8") as _in:
        for i, assembly_data in enumerate(csv.DictReader(_in, delimiter="\t"), start=1):
            if i > 2:
                break
            assembly = Assembly(**assembly_data)
            yield assembly

def register_object(user, pw, obj, obj_type, workdir="work", hold_date=None, dev=True,):
    response = None
    obj_json = workdir / f"{obj_type}.json"

    if obj_json.is_file():
        with open(obj_json, "rt") as _in:
            try:
                response = SubmissionResponse.from_json(_in.read())
            except Exception as err:
                print(f"Reading {obj_type} submission failed.\n\n", err )
                response = None
    if response is None:
        sub = Submission(user, pw, hold_date=hold_date, dev=dev)
        response = sub.submit(obj)
        with open(obj_json, "wt") as _out:
            _out.write(response.to_json())

    print(response)
    yield from response.objects


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("study_json", type=str)
    ap.add_argument("webin_credentials", type=str)
    ap.add_argument("--override", action="store_true",)  # not used at the moment
    ap.add_argument("--workdir", "-w", type=str, default="work")
    ap.add_argument("--hold_date", type=str, default="2025-12-31")
    ap.add_argument("--dryruns", type=int, default=0)
    ap.add_argument("--ena_live", action="store_true")

    args = ap.parse_args()

    run_on_dev_server = not args.ena_live

    user, pw = get_webin_credentials(args.webin_credentials)
    webin_client = EnaWebinClient(user, pw)

    workdir = pathlib.Path(args.workdir)
    if workdir.is_dir():
        if args.override:
            raise NotImplementedError("Workdir override not implemented.")
    else:
        workdir.mkdir(parents=True)

    with open(args.study_json, "rt", encoding="UTF-8",) as json_in:
        study_data = json.load(json_in)

    print(study_data)

    study_json = workdir / "study.json"
    study_id = None
    if False:  # study_json.is_file():
        print("Reading existing study.json...")
        with open(study_json, "rt") as json_in:
            study_response = json.load(json_in)
            study_id = study_response['objects'][0]['accession']

    else:
        study_obj = Study(study_id=study_data["study_id"], study_name=study_data["study_name"], raw_data_projects=study_data["accessions"],)
        print(study_obj)

        # register bioproject
        studies = register_object(user, pw, study_obj, "study", workdir=workdir, hold_date=args.hold_date, dev=run_on_dev_server,)
        studies = list(studies)
        print(*studies, sep="\n")

        study_id = studies[0].accession

    if study_id is None:
        raise ValueError("No study id.")

    # load assembly data and extract samples
    assemblies = {
        f"spire_sample_{assembly['sample_id']}": Assembly(**assembly, spire_ena_project_id=study_id)
        for i, assembly in enumerate(study_data["assemblies"])
        if not run_on_dev_server or (args.dryruns <= 0 or i < args.dryruns)
    }

    sample_json = workdir / "sample.json"
    biosamples = []

    if False:  # sample_json.is_file():
        print("Reading existing sample.json...")
        with open(sample_json, "rt") as json_in:
            sample_response = json.load(json_in)
            biosamples += ((obj["accession"], obj["alias"]) for obj in sample_response["objects"])
    else:
        sample_set = SampleSet()
        sample_set.samples += (assembly.get_sample() for assembly in assemblies.values())
        print(lxml.etree.tostring(sample_set.toxml()).decode())

        # register biosamples
        biosamples = register_object(user, pw, sample_set, "sample", workdir=workdir, hold_date=args.hold_date, dev=run_on_dev_server,)
        biosamples = list(biosamples)
        biosamples = [(obj.accession, obj.alias) for obj in biosamples]

    print(biosamples, sep="\n")
    print(assemblies)
    for accession, alias in biosamples:
        assembly = assemblies.get(alias)
        if assembly is None:
            raise ValueError(f"{alias} does not have an assembly!")
        assembly_dir = workdir / "assemblies" / assembly.assembly_name
        assembly_done = assembly_dir / "DONE"
        if not assembly_done.is_file():
            assembly_dir.mkdir(parents=True, exist_ok=True,)
            with working_directory(assembly_dir):
                manifest = Manifest.from_assembly(assembly, study_id, accession)
                manifest_file = pathlib.Path(f"{assembly.assembly_name}.manifest.txt")
                if not manifest_file.is_file():
                    with open(manifest_file, "wt") as _out:
                        print(manifest.to_str(), file=_out,)
                print(manifest)

                is_valid, messages = webin_client.validate(manifest_file, dev=run_on_dev_server,)
                if is_valid:
                    ena_id, messages = webin_client.submit(manifest_file, dev=run_on_dev_server,)
                    if ena_id:
                        messages = []
                        print("ENA-ID", ena_id,)
                        pathlib.Path("DONE").touch()

                if messages:
                    print(*messages, sep="\n",)


                print("-----------------------------------------------------")



    return None

    # load assembly data and extract samples
    assemblies = {assembly.sample_id: assembly for assembly in parse_assemblies(args.assembly_tsv)}
    sample_set = SampleSet()
    sample_set.samples += (assembly.get_sample() for assembly in assemblies.values())

    print(lxml.etree.tostring(sample_set.toxml()).decode())
    
    # register biosamples
    biosamples = register_object(user, pw, sample_set, "sample", workdir=workdir, hold_date=args.hold_date)
    biosamples = list(biosamples)
    print(biosamples, sep="\n")

    # upload assemblies
    print(assemblies)
    for biosample in biosamples:
        assembly = assemblies.get(biosample.alias)
        if assembly is None:
            raise ValueError(f"{biosample.alias} does not have an assembly!")
        assembly_dir = workdir / "assemblies" / assembly.assembly_name
        assembly_done = assembly_dir / "DONE"
        if not assembly_done.is_file():
            assembly_dir.mkdir(parents=True, exist_ok=True,)
            with working_directory(assembly_dir):
                manifest = Manifest.from_assembly(assembly, studies[0].accession, biosample.accession)
                manifest_file = pathlib.Path(f"{assembly.assembly_name}.manifest.txt")
                if not manifest_file.is_file():
                    with open(manifest_file, "wt") as _out:
                        print(manifest.to_str(), file=_out,)
                print(manifest)

                is_valid, messages = webin_client.validate(manifest_file, dev=True,)
                if is_valid:
                    ena_id, messages = webin_client.submit(manifest_file, dev=True,)
                    if ena_id:
                        messages = []
                        print("ENA-ID", ena_id,)
                        pathlib.Path("DONE").touch()

                if messages:
                    print(*messages, sep="\n",)


                print("-----------------------------------------------------")


    return None


    print(assemblies)
    for biosample in response.objects:
        assembly = assemblies.get(biosample.alias)
        if assembly is not None:
            manifest = Manifest.from_assembly(assembly, ena_study, biosample.accession,)
            print(manifest)
            print("-----------------------------------------------------")
            manifest_file = f"{assembly.assembly_name}.manifest.txt"
            with open(manifest_file, "wt") as _out:
                print(manifest.to_str(), file=_out,)
            is_valid, messages = webin_client.validate(manifest_file, dev=True,)

            if is_valid:
                ena_id, messages = webin_client.submit(manifest_file, dev=False,)

                if ena_id:
                    messages = []
                    print("ENA-ID", ena_id)

            if messages:
                print(*messages, sep="\n",)

        

    return None


    # curl -u 'user:password' -F "SUBMISSION=@submission.xml" -F "STUDY=@study3.xml" "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/"

if __name__ == "__main__":
    main()
