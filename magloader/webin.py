# (magloader) magloader % ena-webin-cli -username Webin-68314 -password 'qvy!qdu9bgv5HVQ6xfq' -context genome -manifest manifest.txt -submit -test
import re
import shlex
import subprocess

LOGLINE_RE = re.compile(r'(^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}) ([A-Z]+ *): (.+)$')


def get_webin_credentials(f):
    with open(f, "rt", encoding="UTF-8") as _in:
        return _in.read().strip().split(":")


class EnaWebinClient:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def _run_client(self, manifest, validate=True, dev=True, java_max_heap=None,):
        mode = "-validate" if validate else "-submit"
        server = "-test" if dev else ""
        jvm_heap = "-Xmx{java_max_heap}" if java_max_heap else ""
        cmd = f"ena-webin-cli {jvm_heap} -username {self.username} -password '{self.password}' -context genome -manifest {manifest} {mode} {server}"
        print(f"CMD: `{cmd}`")

        try:
            proc = subprocess.run(shlex.split(cmd), check=True, capture_output=True,)
        except subprocess.CalledProcessError as err:
            raise

        return proc

    def _evaluate_report(self):
        # 2025-06-06T12:55:57 INFO : Submission(s) validated successfully.
        with open("webin-cli.report", "rt", encoding="UTF-8",) as report:
            for i, line in enumerate(report, start=1):
                logitem = LOGLINE_RE.match(line)
                if not logitem:
                    # raise ValueError("Cannot parse webin-cli.report!")
                    event, message = "UNKNOWN", line
                else:
                    try:
                        event, message = logitem.group(2), logitem.group(3)
                    except IndexError:
                        raise ValueError(f"Log-line {line} has weird format!")

                yield i, event, message

    def validate(self, manifest, dev=True, java_max_heap=None,):
        try:
            proc = self._run_client(manifest, validate=True, dev=dev,)
        except subprocess.CalledProcessError as err:
            raise

        print(proc)
        messages = list(self._evaluate_report())
        if len(messages) == 1 and messages[0][2] == "Submission(s) validated successfully.":
            return True, messages

        # print(*messages, sep="\n")
        return False, messages

    def submit(self, manifest, dev=True,):
        try:
            proc = self._run_client(manifest, validate=False, dev=dev, java_max_heap=None,)
        except subprocess.CalledProcessError as err:
            raise

        print(proc)
        messages = list(self._evaluate_report())
        for _, event, msg in messages:
            if event.strip() == "ERROR":
                break
            if event.strip() == "INFO" and msg.startswith(
                "The submission has been completed successfully. "
                "The following analysis accession was assigned to the submission:"
            ):
                return msg.strip().split(" ")[-1], messages

        # print(*messages, sep="\n")
        return None, messages
