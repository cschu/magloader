import argparse
import json
import os
import pathlib
import pprint
import re

import psycopg2
import pymongo

from ..query.attributes import get_attributes

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
	ap.add_argument("mongodb_json", type=str)
	ap.add_argument("db_json", type=str)
	ap.add_argument("--workdir", "-w", type=str, default="work")
	ap.add_argument("--mag_dir", type=str, default="mags")
	# ap.add_argument("--study_type", choices=("ena", "mg-rast", "metasub", "internal", "ena_mg-rast",), default="ena",)
	args = ap.parse_args()

	mag_dir = pathlib.Path(args.mag_dir)
	mag_dir.mkdir(exist_ok=True, parents=True,)

	with open(args.db_json, "rt", encoding="UTF-8",) as json_in:
		db = json.load(json_in)
	connection = psycopg2.connect(**db)
	cursor = connection.cursor()

	client, dbname = get_client(args.mongodb_json)
	mongo_db = client[dbname]

	with open(args.study_json, "rt") as _in:
		study_d = json.load(_in)
		samples = {
			f"spire_assembly_{item['sample_id']}": (item["sample_id"], item["biosamples"].replace(";", ",").split(","),)
			for item in study_d["assemblies"]
		}
	
	assembly_dir = pathlib.Path(args.workdir) / "assemblies"
	_, dirs, _ = next(os.walk(assembly_dir))

	mags = {}

	for d in dirs:

		with open(assembly_dir / d / f"{d}.manifest.txt", "rt") as _in:
			manifest = dict(
				re.split(' {3}', line.strip())
				for line in _in
			)
		# ../prod/studies/10/work/assemblies/spire_assembly_687/webin-cli.report
		with open(assembly_dir / d / "webin-cli.report", "rt") as _in:
			erz_match = re.search(r'The following analysis accession was assigned to the submission: (ERZ[0-9]+)', _in.read())
			try:
				erz_id = erz_match.group(1)
			except AttributeError:
				raise ValueError("NO ERZ MATCH!")

		spire_sample_id, biosamples = samples.get(manifest["ASSEMBLYNAME"])
		sample_d = {
			"assemblyname": d,
			"biosamples": biosamples,
			"spire_vstudy": manifest.get("STUDY"),
			"spire_vsample": manifest.get("SAMPLE"),
		}

		if len(sample_d["biosamples"]) > 1:
			raise ValueError("TOO MANY BIOSAMPLES")
		

		bins = list(mongo_db.bins.find({"sample_id": sample_d["biosamples"][0]}))
		
		# pprint.pprint(sample_d)
		if bins:
			# pprint.pprint(sample_d)
			for spire_bin in bins:
				# pprint.pprint(spire_bin)
				
				sample_attribs = get_attributes(
						spire_bin,
						biosamples,
						sample_d["assemblyname"],
						study_d["study_id"],
						spire_sample_id,
						sample_d["spire_vstudy"],
						erz_id,)

				bin_data = { 			
        			"mag_id": spire_bin.get("formatted_spire_id"),
        			"bin_id": spire_bin.get("bin_id"),
					"bin_path": spire_bin.get("bin_path"),
				}
				bin_id = bin_data.get("bin_id")
				mags.setdefault(spire_sample_id, {})[bin_id] = {}
				mags[spire_sample_id][bin_id].update(sample_d)
				mags[spire_sample_id][bin_id].update(bin_data)
				mags[spire_sample_id][bin_id]["attribs"] = sample_attribs

				cursor.execute(
					"SELECT average_bin_coverage.avg_coverage "
					"FROM bins "
					"LEFT OUTER JOIN average_bin_coverage "
					"ON bins.id = average_bin_coverage.bin_id "
					f"WHERE bins.bin_name = '{bin_id}';"
				)
				coverage = list(cursor.fetchall())[0] or -1.0
				mags[spire_sample_id][bin_id]["coverage"] = float(coverage)
				
				pprint.pprint(mags)
				
				break
			break	
		# break



if __name__ == "__main__":
	main()