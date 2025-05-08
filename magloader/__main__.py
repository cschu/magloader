import argparse
import csv

import lxml.etree

from .sample import Sample, SampleSet
from .study import Study
from .submission import Submission


def parse_studies(f):
    with open(f, "rt", encoding="UTF-8") as _in:
        for study_data in csv.DictReader(_in, delimiter="\t"):
            study = Study(**study_data)
            print(study)
            yield study

def get_webin_credentials(f):
    with open(f, "rt", encoding="UTF-8") as _in:
        return _in.read().strip().split(":")
    
def parse_assemblies(f):
    with open(f, "rt", encoding="UTF-8") as _in:
        for i, assembly_data in enumerate(csv.DictReader(_in, delimiter="\t"), start=1):
            if i > 2:
                break
            sample = Sample(
                **{
                    k: v
                    for k, v in assembly_data.items()
                    if k in Sample.__match_args__
                }
            )
            # print(sample)
            # print(lxml.etree.tostring(sample.toxml()).decode())
            yield sample
            
            



def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("study_tsv", type=str)
    ap.add_argument("assembly_tsv", type=str)
    ap.add_argument("webin_credentials", type=str)

    args = ap.parse_args()

    user, pw = get_webin_credentials(args.webin_credentials)
    sub = Submission(user, pw, hold_date="2024-10-31", dev=True)

    sample_set = SampleSet()
    sample_set.samples += parse_assemblies(args.assembly_tsv)

    print(lxml.etree.tostring(sample_set.toxml()).decode())

    response = sub.submit(sample_set)
    print(response)

    return None

    for study in parse_studies(args.study_tsv):
        response = sub.submit(study)
        print(response)

    # curl -u 'user:password' -F "SUBMISSION=@submission.xml" -F "STUDY=@study3.xml" "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/"

if __name__ == "__main__":
    main()
