import argparse
import json
import os
import pathlib
import pprint

import psycopg2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("depths_file", type=str)
    ap.add_argument("db_json", type=str)
    ap.add_argument("output_file", type=str)
    args = ap.parse_args()
    
    with open(args.db_json, "rt", encoding="UTF-8",) as json_in:
        db = json.load(json_in)

    connection = psycopg2.connect(**db)
    # print("connected")

    cursor = connection.cursor()
    # print("got cursor")



    contigs = {}
    with open(args.depths_file, "rt") as _in:
        for i, line in enumerate(_in.read().strip().split("\n")):
            if i:
                contig, _, depth, *_ = line.split("\t")
                contigs[contig] = float(depth)
    
    depths_file_basename = pathlib.Path(args.depths_file).name
    sample_name = depths_file_basename[:depths_file_basename.find("_aligned_to")]


    cursor.execute(f"SELECT id FROM samples WHERE sample_name = '{sample_name}';")
    sample_id = cursor.fetchall()[0][0]

    # print(sample_id)

    cursor.execute(
        "SELECT "
        "CONCAT('k',kmer_size,'_',contig_ordinal) as contig, "
        "bins.id, "
        "bins.bin_name, "
        "samples.sample_name "
        "FROM "
        "contigs "
        "JOIN bins ON contigs.bin_id = bins.id "
        "JOIN samples ON samples.id = bins.sample_id "
        "WHERE "
        f"bins.sample_id = {sample_id};"
    )

    bin_coverage = {}
    bins = {}

    for contig, bin_id, bin_name, sample in cursor.fetchall():
        bin_coverage.setdefault(bin_id, []).append(contigs.get(contig))
        bins[bin_id] = bin_name
    

    with open(args.output_file, "wt") as _out:
        for bin_id, depths in bin_coverage.items():
            try:
                avg_coverage = sum(depths) / len(depths)
            except ZeroDivisionError:
                avg_coverage = "NaN"
            except TypeError:
                avg_coverage = "NaN"
            # print(bin_id, bins.get(bin_id), avg_coverage, sep="\t", file=_out)
            print(bin_id, avg_coverage, sep="\t", file=_out)


if __name__ == "__main__":
    main()
