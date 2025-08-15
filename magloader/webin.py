# (magloader) magloader % ena-webin-cli -username Webin-68314 -password 'qvy!qdu9bgv5HVQ6xfq' -context genome -manifest manifest.txt -submit -test
import pathlib
import re
import shlex
import subprocess

LOGLINE_RE = re.compile(r'(^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}) ([A-Z]+ *): (.+)$')

# (4, 'ERROR', 'In analysis, alias: "webin-genome-spire_assembly_46121". The object being added already exists in the submission account with accession: "ERZ27247457". The submission has failed because of a system error.')
RECORD_EXISTS_RE = re.compile(
	r'In analysis, alias: "webin-genome-(.+)"\. '
	r'The object being added already exists in the submission account with accession: "(.+)"\. The submission has failed because of a system error.'
)

def get_webin_credentials(f):
    with open(f, "rt", encoding="UTF-8") as _in:
        return _in.read().strip().split(":")


class EnaWebinClient:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def _run_client(self, manifest, validate=True, dev=True, java_max_heap=None, use_ascp=False,):
        mode = "-validate" if validate else "-submit"
        server = "-test" if dev else ""
        ascp = "-ascp" if use_ascp else ""
        jvm_heap = f"-Xmx{java_max_heap}" if java_max_heap else ""
        cmd = f"ena-webin-cli {jvm_heap} -username {self.username} -password '{self.password}' -context genome -manifest {manifest} {mode} {server} {ascp}"
        print(f"CMD: `{cmd}`")

        try:
            proc = subprocess.run(shlex.split(cmd), check=True, capture_output=True,)
        except subprocess.CalledProcessError as err:
            pass

        return proc

    def _evaluate_report(self):
        # 2025-06-06T12:55:57 INFO : Submission(s) validated successfully.
        webin_cli_report = pathlib.Path("webin-cli.report")
        if webin_cli_report.is_file():
            with open(webin_cli_report.name, "rt", encoding="UTF-8",) as report:
                for i, line in enumerate(report, start=1):
                    logitem = LOGLINE_RE.match(line)
                    if not logitem:
                        event, message = "UNKNOWN", line
                    else:
                        try:
                            event, message = logitem.group(2), logitem.group(3)
                        except IndexError:
                            event, message = "LOGERROR", line

                    yield i, event.strip(), message.strip()
        else:
            yield -1, "NOREPORT", ""

    def validate(self, manifest, dev=True, java_max_heap=None,):
        try:
            proc = self._run_client(manifest, validate=True, dev=dev, java_max_heap=java_max_heap,)
        except subprocess.CalledProcessError as err:
            print("CAUGHT CALLED_PROCESS_ERROR:\n", err)
        else:
            print("PROC", proc)
        finally:
            messages = list(self._evaluate_report())
        
        if len(messages) == 1 and messages[0][2] == "Submission(s) validated successfully.":
            return True, messages

        return False, messages

    def submit(self, manifest, dev=True, java_max_heap=None, use_ascp=False,):
        try:
            proc = self._run_client(manifest, validate=False, dev=dev, java_max_heap=java_max_heap, use_ascp=use_ascp,)
        except subprocess.CalledProcessError as err:
            print("CAUGHT CALLED_PROCESS_ERROR:\n", err)
        else:
            print("PROC", proc)
        finally:
            messages = list(self._evaluate_report())
        
        for _, event, msg in messages:
            if event == "ERROR":
                record_exists = RECORD_EXISTS_RE.match(msg)
                if record_exists:
                    return record_exists.group(2), messages
                # 2025-07-02T14:43:24 ERROR: Invalid field value. Non-negative float expected. [manifest file: /g/bork6/schudoma/projects/spire/upload/prod/studies/260/work/assemblies/spire_assembly_46745/spire_assembly_46745.manifest.txt, line number: 9, field: COVERAGE, value: -1.0]
                break
            if event == "INFO" and msg.startswith(
                "The submission has been completed successfully. "
                "The following analysis accession was assigned to the submission:"
            ):
                return msg.split(" ")[-1], messages

        return None, messages
