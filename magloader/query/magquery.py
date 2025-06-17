import argparse
import json
import pprint

import psycopg2


def main():
	ap = argparse.ArgumentParser()
	ap.add_argument("study_id", type=str)
	ap.add_argument("db_json", type=str)
	args = ap.parse_args()

	

	with open(args.db_json, "rt") as json_in:
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

	cursor.execute(
		"" \
		"SELECT " \
		"samples.id as sample_id, " \
		"samples.sample_name, " \
		"assemblies.id as assembly_id, " \
		"software.software_name, " \
		"software.software_version, " \
		"ena.sample_accession, " \
		"average_coverage.avg_coverage " \
		"FROM " \
		"samples " \
		"JOIN assemblies on samples.id = assemblies.sample_id " \
		"JOIN software on assemblies.assembler=software.id " \
		"JOIN ena on ena.sample_id=samples.id " \
		"JOIN average_coverage on average_coverage.sample_id = samples.id " \
		f"WHERE samples.study_id = {args.study_id};"
	)

	assemblies = {}
	for sample_id, sample_name, assembly_id, program, program_version, sample_accession, coverage in cursor.fetchall():
		assemblies.setdefault(sample_id, {}).update(
			{	
				"sample_id": sample_id,			
				"program": program,
				"program_version": program_version,
				"coverage": float(coverage),
				"file_path": f"/g/scb/bork/data/spire/studies/{args.study_id}/psa_megahit/assemblies/{sample_name}-assembled.fa.gz"
			}
		)
		assemblies[sample_id].setdefault("biosamples", []).append(sample_name)
	
	for assembly in assemblies.values():
		assembly["biosamples"] = ";".join(assembly["biosamples"])

	json_d["assemblies"] = list(assemblies.values())

	pprint.pprint(json_d)

	with open(f"spire_study_{args.study_id}.json", "wt") as json_out:
		json.dump(json_d, json_out)


if __name__ == "__main__":
	main()
