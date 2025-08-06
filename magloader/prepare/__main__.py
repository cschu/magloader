import argparse
import json
import os
import pathlib
import re

import pymongo

"""
f=$1
study=$(echo $f | cut -f 2 -d /)
assembly=$(basename $(dirname $f))
# sample=$(grep "Uploading file" $f | grep -o "[^/]\+$" | sed "s/-assembled.fa.gz//")
accession=$(grep -o "ERZ.\+" $f)

manifest=$(ls $(dirname $f)/*manifest.txt)
estudy=$(grep "^STUDY" $manifest | awk '{print $2}')
esample=$(grep "^SAMPLE" $manifest | awk '{print $2}')
sample=$(grep "^FASTA" $manifest | grep -o "[^/]\+$" | sed "s/-assembled.fa.gz//")
"""

def get_client(f):
	with open(f, "rt", encoding="UTF-8") as _in:
		db = json.load(_in)

	return pymongo.MongoClient(
		f"mongodb://{db['username']}:{db['password']}@{db['host']}:{db['port']}",
	), db['db']


def main():
	ap = argparse.ArgumentParser()
	ap.add_argument("study_json", type=str)
	ap.add_argument("db_json", type=str)
	ap.add_argument("--workdir", "-w", type=str, default="work")
	args = ap.parse_args()

	client, dbname = get_client(args.db_json)
	db = client[dbname]

	with open(args.study_json, "rt") as _in:
		json_d = json.load(_in)
		samples = {
			f"spire_assembly_{item['sample_id']}": item["biosamples"].replace(";", ",").split(",")
			for item in json_d["assemblies"]
		}
	
	assembly_dir = pathlib.Path(args.workdir) / "assemblies"
	_, dirs, _ = next(os.walk(assembly_dir))

	for d in dirs:
		with open(assembly_dir / d / f"{d}.manifest.txt", "rt") as _in:
			manifest = dict(
				re.split(' {3}', line.strip())
				for line in _in
			)
		print(samples.get(manifest["ASSEMBLYNAME"]))
		print(manifest)
		break


	{"study_id": 2, "study_name": "AguirreVonWobeser_2021_avocado", "accessions": "PRJNA656796", 
  "assemblies": [
	  {"sample_id": 121, "program": "megahit", "program_version": "1.2.9", "coverage": 14.9429, "file_path": "/g/bork6/schudoma/projects/spire/upload/prod/studies/2/assemblies/SAMN15803490-assembled.fa.gz", "biosamples": "SAMN15803490"}, 
	  {"sample_id": 118, "program": "megahit", "program_version": "1.2.9", "coverage": 8.62278, "file_path": "/g/bork6/schudoma/projects/spire/upload/prod/studies/2/assemblies/SAMN15803491-assembled.fa.gz", "biosamples": "SAMN15803491"}, {"sample_id": 123, "program": "megahit", "program_version": "1.2.9", "coverage": 0.0925207, "file_path": "/g/bork6/schudoma/projects/spire/upload/prod/studies/2/assemblies/SAMN15803492-assembled.fa.gz", "biosamples": "SAMN15803492"}, {"sample_id": 115, "program": "megahit", "program_version": "1.2.9", "coverage": 15.4185, "file_path": "/g/bork6/schudoma/projects/spire/upload/prod/studies/2/assemblies/SAMN15803493-assembled.fa.gz", "biosamples": "SAMN15803493"}, {"sample_id": 119, "program": "megahit", "program_version": "1.2.9", "coverage": 17.8169, "file_path": "/g/bork6/schudoma/projects/spire/upload/prod/studies/2/assemblies/SAMN15803494-assembled.fa.gz", "biosamples": "SAMN15803494"}, {"sample_id": 117, "program": "megahit", "program_version": "1.2.9", "coverage": 18.6551, "file_path": "/g/bork6/schudoma/projects/spire/upload/prod/studies/2/assemblies/SAMN15803495-assembled.fa.gz", "biosamples": "SAMN15803495"}, {"sample_id": 122, "program": "megahit", "program_version": "1.2.9", "coverage": 17.9407, "file_path": "/g/bork6/schudoma/projects/spire/upload/prod/studies/2/assemblies/SAMN15803496-assembled.fa.gz", "biosamples": "SAMN15803496"}, {"sample_id": 116, "program": "megahit", "program_version": "1.2.9", "coverage": 0.0947402, "file_path": "/g/bork6/schudoma/projects/spire/upload/prod/studies/2/assemblies/SAMN15803497-assembled.fa.gz", "biosamples": "SAMN15803497"}, {"sample_id": 120, "program": "megahit", "program_version": "1.2.9", "coverage": 9.39223, "file_path": "/g/bork6/schudoma/projects/spire/upload/prod/studies/2/assemblies/SAMN15803498-assembled.fa.gz", "biosamples": "SAMN15803498"}]}(magloader_mag) [schudoma@login1 magloader]$ ls ..
	



if __name__ == "__main__":
	main()