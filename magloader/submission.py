import lxml.etree, lxml.builder
import requests

from dataclasses import dataclass
from io import StringIO


@dataclass
class SubmissionResponse:
    receipt_date: str = None
    success: bool = False
    alias: str = None
    # obj_type: str = None
    accession: str = None
    ext_accession: str = None
    status: str = None
    hold_until: str = None
    messages: list = None
    submission_alias: str = None
    submission_accession: str = None


class Submission:
    def __init__(self, user, pw, hold_date=None, dev=True, timeout=60):
        self.user = user
        self.pw = pw
        self.hold_date = hold_date
        self.dev = dev
        self.timeout = timeout

    def get_auth(self):
        return self.user, self.pw
    
    def submit(self, obj):
        # requests.post(url, files={"SUBMISSION": open("submission.xml", "rb"), "STUDY": open("study3.xml", "rb")}, auth=(webin, pw))
        url = f"https://www{('', 'dev')[self.dev]}.ebi.ac.uk/ena/submit/drop-box/submit/"
        response = requests.post(
            url,
            files={
                "SUBMISSION": StringIO(Submission.generate_submission(hold_date=self.hold_date)),
                obj.__class__.__name__.upper().replace("SET", ""): StringIO(
                    lxml.etree.tostring(obj.toxml()).decode()
                ),
            },
            auth=self.get_auth(),
            timeout=self.timeout,
        )

        return obj.parse_submission_response(response)


    @staticmethod
    def generate_submission(hold_date=None):
        maker = lxml.builder.ElementMaker()

        submission = maker.SUBMISSION
        actions = maker.ACTIONS
        action = maker.ACTION
        add = maker.ADD
        hold = maker.HOLD

        action_list = [action(add()),]
        if hold_date is not None:
            action_list.append(action(hold(HoldUntilDate=hold_date)))

        doc = submission(actions(*action_list))
        return lxml.etree.tostring(doc).decode()