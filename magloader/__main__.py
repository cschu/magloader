#!/usr/bin/env python

import argparse
import contextlib
import json
import os
import pathlib

from functools import partial
from multiprocessing import Pool

import lxml.etree

from .assembly import Assembly
from .manifest import Manifest
from .sampleset import SampleSet
from .sample import MagSample
from .study import Study, STUDY_TYPES
from .submission import Submission, SubmissionResponse
from .upload import check_assemblies, prepare_manifest_files, process_manifest, upload
from .webin import get_webin_credentials, EnaWebinClient
from .workdir import working_directory




def register_object(user, pw, obj, obj_type, hold_date=None, dev=True, timeout=60,):
    response = None
    obj_json = pathlib.Path(f"{obj_type}_response.json")

    if obj_json.is_file():
        with open(obj_json, "rt") as _in:
            try:
                response = SubmissionResponse.from_json(_in.read())
            except Exception as err:
                print(f"Reading {obj_type} submission failed.\n\n", err )
                response = None
    if response is None:
        sub = Submission(user, pw, hold_date=hold_date, dev=dev, timeout=timeout,)
        response = sub.submit(obj)
        with open(obj_json, "wt") as _out:
            _out.write(response.to_json())

    print(response)

    yield from response.objects


def register_samples(sample_set, workdir, sample_dir, user, pw, hold_date, run_on_dev_server, timeout):
    # register biosamples
    biosamples = []
    sample_dir = pathlib.Path(workdir / sample_dir)
    sample_dir.mkdir(exist_ok=True, parents=True,)
    with working_directory(sample_dir):
        biosamples = register_object(user, pw, sample_set, "sample", hold_date=hold_date, dev=run_on_dev_server, timeout=timeout,)
        biosamples = list(biosamples)

    print(biosamples, sep="\n")
    return biosamples



def register_study(study_data, workdir, user, pw, hold_date, run_on_dev_server):
    print(study_data)

    study_id = None

    StudyType = STUDY_TYPES.get(study_data.get("study_type", "ena"))

    study_obj = StudyType(study_id=study_data["study_id"], raw_data_projects=study_data["accessions"],)
    print(study_obj)

    # register bioproject
    study_dir = pathlib.Path(workdir / "study")
    study_dir.mkdir(exist_ok=True, parents=True,)
    with working_directory(study_dir):
        studies = register_object(user, pw, study_obj, "study", hold_date=hold_date, dev=run_on_dev_server,)
        studies = list(studies)
    print(*studies, sep="\n")

    study_id = studies[0].accession

    if study_id is None:
        raise ValueError("No study id.")
    
    return study_id


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("input_json", type=str)
    ap.add_argument("webin_credentials", type=str)
    ap.add_argument("--override", action="store_true",)  # not used at the moment
    ap.add_argument("--workdir", "-w", type=str, default="work")
    ap.add_argument("--hold_date", type=str, default="2025-12-31")
    ap.add_argument("--dryruns", type=int, default=0)
    ap.add_argument("--ena_live", action="store_true")
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--java_max_heap", type=str, default=None,)
    ap.add_argument("--timeout", type=int, default=60,)

    args = ap.parse_args()

    run_on_dev_server = not args.ena_live

    user, pw = get_webin_credentials(args.webin_credentials)
    
    workdir = pathlib.Path(args.workdir)
    if workdir.is_dir():
        if args.override:
            raise NotImplementedError("Workdir override not implemented.")
    else:
        workdir.mkdir(parents=True)

    with open(args.input_json, "rt", encoding="UTF-8",) as json_in:
        input_data = json.load(json_in)


    if input_data.get("study_id"):
        study_id = register_study(input_data, workdir, user, pw, args.hold_date, run_on_dev_server,)

        # load assembly data and extract samples
        assemblies = {
            f"spire_sample_{assembly['sample_id']}": Assembly(**assembly, spire_ena_project_id=study_id)
            for i, assembly in enumerate(input_data["assemblies"])
            if not run_on_dev_server or (args.dryruns <= 0 or i < args.dryruns)
        }
        sample_set = SampleSet()
        sample_set.samples += (assembly.get_sample() for assembly in assemblies.values())

        print(lxml.etree.tostring(sample_set.toxml()).decode())

        biosamples = register_samples(sample_set, workdir, "samples", user, pw, args.hold_date, run_on_dev_server, args.timeout)


        # validate and submit assemblies
        print(assemblies)

        assemblies = list(check_assemblies(biosamples, assemblies))
        manifests = list(prepare_manifest_files(study_id, assemblies, workdir))

        process_manifest_partial = partial(
            process_manifest,
            user=user,
            password=pw,
            submit=True,
            run_on_dev_server=run_on_dev_server,
            java_max_heap=args.java_max_heap,
        )
        print(process_manifest_partial)

        with open("assembly_accessions.txt", "wt") as _out:
            for i, ena_id, messages, manifest in upload(manifests, process_manifest_partial, threads=args.threads):
                if ena_id is not None:
                    print(i, i/len(manifests), "ENA-ID", ena_id,)
                    print(ena_id, manifest, sep="\t", file=_out)
                else:
                    print(i, i/len(manifests), *messages, sep="\n",)
                print("-----------------------------------------------------")


    elif input_data.get("vstudy_id"):
        study_id = input_data["vstudy_id"]

        sample_set = SampleSet()

        assemblies = {}
        for i, (bin_id, mag) in enumerate(input_data["mags"].items()):
            if not run_on_dev_server or (args.dryruns <= 0 or i < args.dryruns):
                assemblies[mag["mag_id"]] = Assembly(
                    spire_ena_project_id=study_id,
                    sample_id=input_data["spire_sample"],
                    assembly_name=mag["mag_id"],
                    assembly_type="Metagenome-Assembled Genome (MAG)",
                    program=mag["program"],
                    file_path=mag["bin_path"],
                    coverage=mag["coverage"],
                    biosamples=mag["biosamples"],
                    program_version=mag["program_version"],
                )
                sample_set.samples.append(
                    MagSample(
                        spire_ena_project_id=study_id,
                        sample_id=mag["mag_id"],
                        biosamples=mag["biosamples"],
                        attributes=mag["attribs"],
                    )
                )

        print(lxml.etree.tostring(sample_set.toxml()).decode())
        biosamples = register_samples(sample_set, workdir, f"vsamples/{input_data['spire_sample']}", user, pw, args.hold_date, run_on_dev_server, args.timeout)

        # validate and submit assemblies
        print(assemblies)

        assemblies = list(check_assemblies(biosamples, assemblies))


    return None


if __name__ == "__main__":
    main()
