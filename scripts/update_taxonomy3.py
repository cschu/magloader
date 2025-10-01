#!/usr/bin/env python

import argparse
import pathlib
import re
import time

import lxml.etree as ET
import lxml.builder 

from magloader.sampleset import SampleSet
from magloader.submission import Submission
from magloader.webin import get_webin_credentials
from magloader.workdir import working_directory

# 1062629-24-0-0.psa_megahit.psb_metabat2.00001 spire_mag_01816750  ERS26731228 d__Bacteria;p__Bacillota_C;c__Negativicutes;o__Veillonellales;f__Dialisteraceae;g__Dialister;s__Dialister invisus   d__Bacteria;p__Firmicutes;c__Negativicutes;o__Veillonellales;f__Veillonellaceae;g__Dialister;s__Dialister invisus   Dialister invisus   218538:
def read_taxonomies(f):
    with open(f, "rt") as _in:
        return dict(
            (line.strip().split("\t") + ["NOTFOUND:-1",])[1::5]
            for line in _in
        ) 

def read_samples(f):
    d = {}
    with open(f, "rt") as _in:
        for line in _in:
            sample, path = line.strip().split("\t")
            d.setdefault(path, {})[sample] = False
    return d



def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("samples", type=str)
    ap.add_argument("taxonomies", type=str)
    ap.add_argument("webin_credentials", type=str)
    ap.add_argument("--workdir", "-w", type=str, default="work")
    ap.add_argument("--hold_date", type=str)
    ap.add_argument("--ena_live", action="store_true")
    ap.add_argument("--timeout", type=int, default=300,)
    ap.add_argument("--force", action="store_true")
    
    args = ap.parse_args()

    user, pw = get_webin_credentials(args.webin_credentials)
    run_on_dev_server = not args.ena_live

    taxdict = read_taxonomies(args.taxonomies)
    
    samples = read_samples(args.samples)

    workdir = pathlib.Path(args.workdir)
    workdir.mkdir(exist_ok=True, parents=True,)

    parser = ET.XMLParser(remove_blank_text=True,)

    maker = lxml.builder.ElementMaker()

    # /g/bork6/schudoma/projects/spire/upload/prod/studies/102/work/vsamples/13230/sampleset.xml
    # /g/bork6/schudoma/projects/spire/upload/magloader/taxonomy_update_20250925/vsamples/23/2737/sampleset.xml
    
    for path, mags in samples.items():
        study = re.search(r"studies/([0-9]+)", path)
        # study = re.search(r"vsamples/([0-9]+)", path)
        if study is None:
            continue
        study = study.group(1)
        sample = re.search(r"vsamples/([0-9]+)", path)
        # sample = re.search(r"vsamples/[0-9]+/([0-9]+)", path)
        if sample is None:
            continue
        sample = sample.group(1)

        sample_dir = workdir / "vsamples" / study / sample
        done_sentinel = sample_dir / "DONE"
        if done_sentinel.is_file() and not args.force:
            continue
        sample_dir.mkdir(exist_ok=True, parents=True,)

        update_samples = []
        with open(path, "rt") as _in:
            tree = ET.fromstring(_in.read())
            for s in tree.findall("SAMPLE"):
                alias = s.attrib.get("alias") 
                if mags.get(alias) is not None:
                    taxid = taxdict.get(alias)
                    if taxid is None:
                        continue
                    title = s.find("TITLE")
                    # title.text = re.sub(r"(;?[dpcofg]__;|s__$)", "", title.text)
                    # title.text = re.sub(r"(.+)(classified as )(.+;)?([^;]+)$", r"\1\2\3", title.text)
                    # lineage = re.sub(r"(;?[dpcofg]__;|s__$)", "", re.search("d__.+$", title.text).group(0))
                    lineage_match = re.search("d__.+$", title.text)
                    if lineage_match is None:
                        title.text = f"Metagenome-Assembled Genome {alias} in SPIRE v01, unclassified"
                    else:
                        taxon = re.sub(r"^[dpcofgs]__", "", re.sub(r"[^;]+;", "", re.sub(r"(;?[dpcofg]__;|;s__$)", "", lineage_match.group(0))))
                    
                        # title.text = re.sub(r"[dpcofgs]__", "", re.sub(r"(.+)(classified as )(.+;)?([^;]+)$", r"\1\2\4", re.sub(r"(;?[dpcofg]__;|s__$)", "", title.text))).replace(",", "")
                        title.text = f"Metagenome-Assembled Genome {alias} in SPIRE v01 classified as {taxon}"
                    
                    #sed "s/\(;\?[dpcofg]__;\|s__$\)//g"
                    

                    n = s.find("SAMPLE_NAME")
                    t = n.find("TAXON_ID")
                    if t.text == "256318":
                        t.text = taxid
                        update_samples.append(s)
                        print(alias, path, taxid)
            if not update_samples:
                continue 
            with working_directory(sample_dir):
                
                #tree = ET.fromstring(ET.tostring(tree), parser)
                sample_set = maker.SAMPLE_SET(*(s for s in update_samples))

                sub = Submission(user, pw, hold_date=None, dev=run_on_dev_server, timeout=args.timeout,)
                response = sub.submit(obj=SampleSet(), modify=True, xml=sample_set,)
                with open(f"sampleset.modify.json", "wt") as _out:
                    _out.write(response.to_json())
                print(response)
                if response.success:
                    pathlib.Path("DONE").touch()
                # time.sleep(3)


        #break

    
    


if __name__ == "__main__":
    main()
