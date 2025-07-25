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
from .sample import SampleSet
from .study import Study
from .submission import Submission, SubmissionResponse
from .upload import check_assemblies, prepare_manifest_files, process_manifest, upload
from .webin import get_webin_credentials, EnaWebinClient
from .workdir import working_directory




def register_object(user, pw, obj, obj_type, hold_date=None, dev=True,):
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
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--java_max_heap", type=str, default=None,)

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

    study_id = None

    study_obj = Study(study_id=study_data["study_id"], raw_data_projects=study_data["accessions"],)
    print(study_obj)

    # register bioproject
    study_dir = pathlib.Path(workdir / "study")
    study_dir.mkdir(exist_ok=True, parents=True,)
    with working_directory(study_dir):
        studies = register_object(user, pw, study_obj, "study", hold_date=args.hold_date, dev=run_on_dev_server,)
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

    biosamples = []

    sample_set = SampleSet()
    sample_set.samples += (assembly.get_sample() for assembly in assemblies.values())
    print(lxml.etree.tostring(sample_set.toxml()).decode())

    # register biosamples
    sample_dir = pathlib.Path(workdir / "samples")
    sample_dir.mkdir(exist_ok=True, parents=True,)
    with working_directory(sample_dir):
        biosamples = register_object(user, pw, sample_set, "sample", hold_date=args.hold_date, dev=run_on_dev_server,)
        biosamples = list(biosamples)

    print(biosamples, sep="\n")

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

    return None


if __name__ == "__main__":
    main()
