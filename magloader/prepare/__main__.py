import argparse
import json
import os
import pathlib
import re

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

def main():
	ap = argparse.ArgumentParser()
	ap.add_argument("study_json", type=str)
	ap.add_argument("--workdir", "-w", type=str, default="work")

	args = ap.parse_args()

	with open(args.study_json, "rt") as _in:
		json_d = json.load(_in)
	
	assembly_dir = pathlib.Path(args.workdir) / "assemblies"
	pwd, dirs, files = next(os.walk(assembly_dir))

	for d in dirs:
		with open(assembly_dir / d / f"{d}.manifest.txt", "rt") as _in:
			manifest = dict(
				re.split(' {3}', line.strip())
				for line in _in
			)
		print(manifest)
		break
	



if __name__ == "__main__":
	main()