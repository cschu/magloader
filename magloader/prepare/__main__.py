import argparse
import json
import os
import pathlib
import pprint
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
		study_d = json.load(_in)
		samples = {
			f"spire_assembly_{item['sample_id']}": item["biosamples"].replace(";", ",").split(",")
			for item in study_d["assemblies"]
		}
	
	assembly_dir = pathlib.Path(args.workdir) / "assemblies"
	_, dirs, _ = next(os.walk(assembly_dir))

	for d in dirs:

		with open(assembly_dir / d / f"{d}.manifest.txt", "rt") as _in:
			manifest = dict(
				re.split(' {3}', line.strip())
				for line in _in
			)
		sample_d = {
			"biosamples": samples.get(manifest["ASSEMBLYNAME"]),
			"spire_vstudy": manifest.get("STUDY"),
			"spire_vsample": manifest.get("SAMPLE"),
		}
		if len(sample_d["biosamples"]) > 1:
			raise ValueError("TOO MANY BIOSAMPLES")
		bins = list(db.bins.find({"sample_id": sample_d["biosamples"][0]}))
		
		pprint.pprint(sample_d)
		if bins:
			for spire_bin in bins:
				print(spire_bin)
				break
			break	
		# break



if __name__ == "__main__":
	main()