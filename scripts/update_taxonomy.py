#!/usr/bin/env python

import argparse
import pathlib
import re

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
            (line.strip().split("\t") + ["NOTFOUND:-1",])[1::4]
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
    ap.add_argument("--timeout", type=int, default=None,)
    
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
    
    for path, mags in samples.items():
        study = re.search(r"studies/([0-9]+)", path).group(1)
        sample = re.search(r"vsamples/([0-9]+)", path).group(1)

        sample_dir = workdir / "vsamples" / study / sample
        sample_dir.mkdir(exist_ok=True, parents=True,)
        
        with open(path, "rt") as _in:
            tree = ET.fromstring(_in.read())
            for s in tree.findall("SAMPLE"):
                alias = s.attrib.get("alias") 
                if mags.get(alias) is not None:
                    taxon = taxdict.get(alias)
                    if taxon.startswith("SEARCH_NOT_IMPLEMENTED"):
                        continue
                    print(taxon)
                    if ";" in taxon:
                        taxon = taxon.split(";")[0]
                    taxname, taxid = taxon.split(":")
                    n = s.find("SAMPLE_NAME")
                    children = n.getchildren()
                    children[0].text = taxid
                    n.insert(1, maker.SCIENTIFIC_NAME(taxname))
                    n.insert(2, maker.COMMON_NAME(""))
                    
                    print(alias, path, taxon, children)
                    tree = maker.SAMPLE_SET(s)
                    break
            
            with working_directory(sample_dir):
                tree = ET.fromstring(ET.tostring(tree), parser)
                #with open(workdir / "sampleset.xml", "wb") as _out:
                #    # _out.write(ET.tostring(ET.fromstring(ET.tostring(tree), parser), pretty_print=True,))
                #    _out.write(ET.tostring(tree), pretty_print=True,))

                sub = Submission(user, pw, hold_date=None, dev=run_on_dev_server, timeout=args.timeout,)
                response = sub.submit(obj=SampleSet(), modify=True, xml=tree,)
                with open(f"sampleset.modify.json", "wt") as _out:
                    _out.write(response.to_json())

                print(response)

        break

    
    


if __name__ == "__main__":
    main()
