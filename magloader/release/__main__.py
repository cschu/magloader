import argparse

from datetime import datetime

from ..submission import Submission
from ..webin import get_webin_credentials


def main():
	ap = argparse.ArgumentParser()
	ap.add_argument("study_id", type=str)
	ap.add_argument("webin_credentials", type=str)
	ap.add_argument("--workdir", "-w", type=str, default="work")
	ap.add_argument("--hold_date", type=str, default=datetime.today().strftime('%Y-%m-%d'))
	ap.add_argument("--ena_live", action="store_true")
	ap.add_argument("--timeout", type=int, default=None,)
	args = ap.parse_args()

	user, pw = get_webin_credentials(args.webin_credentials)
	run_on_dev_server = not args.ena_live

	sub = Submission(user, pw, hold_date=args.hold_date, dev=run_on_dev_server, timeout=args.timeout,)
	response = sub.submit()
	with open(f"{args.study_id}.submission.json", "wt") as _out:
		_out.write(response.to_json())

	print(response)

	
	


if __name__ == "__main__":
	main()