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
	ap.add_argument("--mags_json_dir", type=str, default="mags_json")
	ap.add_argument("--mag_dir", type=str, default="mags")
	# ap.add_argument("--study_type", choices=("ena", "mg-rast", "metasub", "internal", "ena_mg-rast",), default="ena",)
	args = ap.parse_args()

	mag_dir = pathlib.Path(args.mag_dir)
	mag_dir.mkdir(exist_ok=True, parents=True,)

	mags_json_dir = pathlib.Path(args.mags_json_dir)
	mags_json_dir.mkdir(exist_ok=True, parents=True,)

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


	for d in dirs:
		json_d = {
			"vstudy_id": None,
			"spire_sample": None,
			"mags": {},
		}
		mags = json_d["mags"]

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

		json_d["vstudy_id"] = manifest.get("STUDY")
		json_d["spire_sample"] = spire_sample_id

		bins = list(mongo_db.bins.find({"sample_id": sample_d["biosamples"][0]}))
		
		if bins:
			n_bins = len(bins)
			for i, spire_bin in enumerate(bins, start=1):
				bin_id = spire_bin.get("bin_id")
				print(f"Processing bin {bin_id} ({i}/{n_bins})", flush=True,)
				mag_id = spire_bin.get("formatted_spire_id")

				cursor.execute(
					"SELECT average_bin_coverage.avg_coverage "
					"FROM bins "
					"JOIN versions.spirev1_bins "
					"ON bins.id = versions.spirev1_bins.bin_id "
					"LEFT OUTER JOIN average_bin_coverage "
					"ON bins.id = average_bin_coverage.bin_id "
					f"WHERE bins.bin_name = '{bin_id}' "
					"AND versions.spirev1_bins.included_in_spire;"
				)
				results = list(cursor.fetchall())
				if not results:
					print(f"bin {bin_id} is not included in spire -> discarding")
					n_bins -= 1
					continue

				# coverage = list(cursor.fetchall())[0][0] or -1.0
				try:
					coverage = results[0][0]
				except:
					coverage = -1.0
				
				sample_attribs = get_attributes(
					spire_bin,
					biosamples,
					mag_id,
					study_d["study_id"],
					spire_sample_id,
					sample_d["spire_vstudy"],
					erz_id,
				)
				
				original_bin_path = spire_bin.get("bin_path")

				bin_path = mag_dir / pathlib.Path(original_bin_path).name
				try:
					bin_path.symlink_to(original_bin_path)
				except FileExistsError:
					pass

				bin_data = { 			
        			"mag_id": mag_id,
        			"bin_id": bin_id,
					"bin_path": str(bin_path.absolute()),
				}

				# mags.setdefault(spire_sample_id, {})[bin_id] = {}
				mags[bin_id] = {}
				mags[bin_id].update(sample_d)
				mags[bin_id].update(bin_data)
				mags[bin_id]["attribs"] = sample_attribs

				mags[bin_id]["coverage"] = float(coverage)
				mags[bin_id]["program"] = "megahit"
				mags[bin_id]["program_version"] = "1.2.9"

		spire_study = study_d["study_id"]
		with open(
			mags_json_dir / f"spire_study_{spire_study}_{spire_sample_id}_mags.json", "wt"
		) as json_out:
			json.dump(json_d, json_out, indent=4,)


if __name__ == "__main__":
	main()