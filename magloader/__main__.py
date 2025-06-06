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
from .sample import Sample, SampleSet
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


def parse_studies(f):
    with open(f, "rt", encoding="UTF-8") as _in:
        for study_data in csv.DictReader(_in, delimiter="\t"):
            study = Study(**study_data)
            print(study)
            yield study


    
def parse_assemblies(f):
    with open(f, "rt", encoding="UTF-8") as _in:
        for i, assembly_data in enumerate(csv.DictReader(_in, delimiter="\t"), start=1):
            if i > 2:
                break
            assembly = Assembly(**assembly_data)
            # sample = Sample(
            #     **{
            #         k: v
            #         for k, v in assembly_data.items()
            #         if k in Sample.__match_args__
            #     }
            # )
            # yield sample
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


def register_study(user, pw, study_tsv, workdir="work", hold_date=None, dev=True):
    study_json = workdir / "study.json"
    response = None
    if study_json.is_file():        
        with open(study_json, "rt") as _in:
            try:
                response = SubmissionResponse.from_json(_in.read())
            except Exception as err:
                print("Reading study submission failed.\n\n", err )
                response = None
    
    if response is None:
        sub = Submission(user, pw, hold_date=hold_date, dev=dev)    
        for study in parse_studies(study_tsv):
            response = sub.submit(study)
            with open(study_json, "wt") as _out:
                _out.write(response.to_json())

    print(response)
    return response.objects[0].accession

def register_samples(user, pw, sample_set, workdir="work", hold_date=None, dev=True):
    sample_json = workdir / "samples.json"
    response = None
    if sample_json.is_file():
        with open(sample_json, "rt") as _in:
            try:
                response = SubmissionResponse.from_json(_in.read())
            except Exception as err:
                print("Reading sample submission failed.\n\n", err )
                response = None

    if response is None:
        sub = Submission(user, pw, hold_date=hold_date, dev=dev)
        response = sub.submit(sample_set)
        with open(sample_json, "wt") as _out:
            _out.write(response.to_json())

    print(response)
    yield from response.objects

            



def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("study_tsv", type=str)
    ap.add_argument("assembly_tsv", type=str)
    ap.add_argument("webin_credentials", type=str)
    ap.add_argument("--override", action="store_true",)  # not used at the moment
    ap.add_argument("--workdir", "-w", type=str, default="work")
    ap.add_argument("--hold_date", type=str, default="2025-12-31")

    args = ap.parse_args()

    user, pw = get_webin_credentials(args.webin_credentials)
    webin_client = EnaWebinClient(user, pw)


    rundir = pathlib.Path.cwd()

    workdir = pathlib.Path(args.workdir)
    if workdir.is_dir():
        if args.override:
            raise NotImplementedError("Workdir override not implemented.")
    else:
        workdir.mkdir(parents=True)
    

    # study_json = workdir / "study.json"
    # response = None
    # if study_json.is_file():        
    #     with open(study_json, "rt") as _in:
    #         try:
    #             response = SubmissionResponse.from_json(json.load(_in))
    #         except:
    #             print("Reading study submission failed.")
    #             response = None
    
    # if response is None:
    #     sub = Submission(user, pw, hold_date="2025-12-31", dev=True)    
    #     for study in parse_studies(args.study_tsv):
    #         response = sub.submit(study)
    #         print(response)
    #         with open(study_json, "wt") as _out:
    #             _out.write(response.to_json())

    # ena_study = response.objects[0].accession



    # ena_study = register_study(user, pw, args.study_tsv, workdir=workdir, hold_date=args.hold_date)
    # print(ena_study)

    studies = register_object(user, pw, list(parse_studies(args.study_tsv))[0], "study", workdir=workdir, hold_date=args.hold_date)
    print(*list(studies), sep="\n")



    assemblies = {assembly.sample_id: assembly for assembly in parse_assemblies(args.assembly_tsv)}


    sample_set = SampleSet()
    # sample_set.samples += parse_assemblies(args.assembly_tsv)
    sample_set.samples += (assembly.get_sample() for assembly in assemblies.values())

    print(lxml.etree.tostring(sample_set.toxml()).decode())
    
    # samples = register_samples(user, pw, sample_set, workdir=workdir, hold_date=args.hold_date)
    samples = register_object(user, pw, sample_set, "sample", workdir=workdir, hold_date=args.hold_date)
    print(*list(samples), sep="\n")

    return None

    sub = Submission(user, pw, hold_date="2025-12-31", dev=True)

    

    response = sub.submit(sample_set)
    print(response)


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
