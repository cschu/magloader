import argparse
import json
import os
import pathlib
import pprint

import psycopg2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("study_id", type=str)
    ap.add_argument("db_json", type=str)
    ap.add_argument("--spire_version", type=int, choices=(1,2,), default=1)
    ap.add_argument("--assembly_dir", type=str, default="assemblies")
    args = ap.parse_args()

    assembly_dir = pathlib.Path(args.assembly_dir)
    assembly_dir.mkdir(exist_ok=True, parents=True,)

    with open(args.db_json, "rt", encoding="UTF-8",) as json_in:
        db = json.load(json_in)

    connection = psycopg2.connect(**db)
    print("connected")

    cursor = connection.cursor()
    print("got cursor")

    json_d = {}

    cursor.execute(
        "SELECT DISTINCT "
        "study_id, study_accession, study_name "
        "FROM "
        "("
        "	SELECT ena.study_id, ena.study_accession, studies.study_name "
        "	FROM ena "
        "	JOIN studies ON ena.study_id = studies.id"
        ") AS studies_ena "
        f"WHERE study_id = {args.study_id};"
    )

    json_d["accessions"] = ";".join(
        acc
        for json_d["study_id"], acc, json_d["study_name"]
        in cursor.fetchall()
    )

    if args.spire_version == 1:
        assembly_query = "'null'"
        software_query = "'megahit'"
        software_version_query = "'1.2.9'"
        assembly_join = ""
        software_join = ""
    else:
        assembly_query = "assemblies.id as assembly_id"
        software_query = "software.software_name"
        software_version_query = "software.software_version"
        assembly_join = "JOIN assemblies on samples.id = assemblies.sample_id "
        software_join = "JOIN software on assemblies.assembler = software.id "

    
    cursor.execute(
        "SELECT "
        "samples.id as sample_id, "
        "samples.sample_name, "
        f"{assembly_query}, "
        f"{software_query}, "
        f"{software_version_query}, "
        "ena.sample_accession, "
        "average_coverage.avg_coverage "
        "FROM "
        "samples "
        f"{assembly_join}"
        f"{software_join}"
        "JOIN ena on ena.sample_id = samples.id "
        "LEFT OUTER JOIN average_coverage on average_coverage.sample_id = samples.id "
        f"WHERE samples.study_id = {args.study_id};"
    )

    assemblies = {}
    for sample_id, sample_name, assembly_id, program, program_version, sample_accession, coverage in cursor.fetchall():
        try:
            coverage = float(coverage)
        except:
            coverage = -1.0

        assembly_path = assembly_dir / f"{sample_name}-assembled.fa.gz"
        try:
            assembly_path.symlink_to(f"/g/scb/bork/data/spire/studies/{args.study_id}/psa_megahit/assemblies/{sample_name}-assembled.fa.gz")
        except FileExistsError:
            pass

        assemblies.setdefault(sample_id, {}).update(
            {
                "sample_id": sample_id,			
                "program": program,
                "program_version": program_version,
                "coverage": float(coverage),
                "file_path": str(assembly_path.absolute()),
            }
        )
        assemblies[sample_id].setdefault("biosamples", []).append(sample_name)

    for assembly in assemblies.values():
        assembly["biosamples"] = ";".join(assembly["biosamples"])

    json_d["assemblies"] = list(assemblies.values())

    pprint.pprint(json_d)

    with open(f"spire_study_{args.study_id}.json", "wt", encoding="UTF-8",) as json_out:
        json.dump(json_d, json_out)


if __name__ == "__main__":
    main()
