import copy
import json
import requests

from dataclasses import dataclass, field
from io import StringIO

import lxml.etree, lxml.builder


@dataclass
class SubmissionResponseObject:
    alias: str = None
    object_type: str = None
    accession: str = None
    ext_accession: str = None
    status: str = None
    hold_until: str = None


@dataclass
class SubmissionResponse:

    #     <RECEIPT receiptDate="2025-06-05T09:17:27.485+01:00" submissionFile="SUBMISSION" success="true">
    # <SAMPLE accession="ERS31594040" alias="spire_sample_177" status="PRIVATE" holdUntilDate="2025-12-31Z">
    #     <EXT_ID accession="SAMEA131616364" type="biosample"/>
    # </SAMPLE>
    # <SAMPLE accession="ERS31594041" alias="spire_sample_172" status="PRIVATE" holdUntilDate="2025-12-31Z">
    #     <EXT_ID accession="SAMEA131616365" type="biosample"/>
    # </SAMPLE>
    # <SUBMISSION accession="ERA33153026" alias="SUBMISSION-05-06-2025-09:17:27:066"/>
    # <MESSAGES>
    #     <INFO>All objects in this submission are set to private status (HOLD).</INFO>
    #     <INFO>This submission is a TEST submission and will be discarded within 24 hours</INFO>
    # </MESSAGES>
    # <ACTIONS>ADD</ACTIONS>
    # <ACTIONS>HOLD</ACTIONS>
    # </RECEIPT>
    receipt_date: str = None
    success: bool = False
    objects: list = field(default_factory=list)
    messages: list = None
    submission_alias: str = None
    submission_accession: str = None

    @classmethod
    def from_xml(cls, xml, obj_type):
        # xml = "\n".join(line for line in xml.text.strip().split("\n") if line[:5] != "<?xml")

        tree = lxml.etree.fromstring(xml)

        d = {
            "success": tree.attrib.get("success", "false").lower() != "false",
            "receipt_date": tree.attrib.get("receiptDate"),
            "objects": list(obj_type.parse_submission_response(tree)),
        }

        submission = tree.find("SUBMISSION")
        if submission is not None:
            d["submission_accession"] = submission.attrib.get("accession")
            d["submission_alias"] = submission.attrib.get("alias")

        messages = tree.find("MESSAGES")
        if messages is not None:
            d["messages"] = [(m.tag, m.text) for m in messages.getchildren()]

        return cls(**d)

    def to_json(self):
        d = copy.deepcopy(self.__dict__)
        d['objects'] = [o.__dict__ for o in d['objects']]

        return json.dumps(d)

    @classmethod
    def from_json(cls, json_str):
        obj = SubmissionResponse(**json.loads(json_str))
        obj.objects = [SubmissionResponseObject(**o) for o in obj.objects]
        return obj


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
        # curl -u 'user:password' -F "SUBMISSION=@submission.xml" -F "STUDY=@study3.xml" "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/"
        url = f"https://www{('', 'dev')[self.dev]}.ebi.ac.uk/ena/submit/drop-box/submit/"

        submission_xml = Submission.generate_submission(hold_date=self.hold_date)
        with open("submission.xml", "wt") as _out:
            _out.write(submission_xml)

        obj_xml = obj.toxml()

        # obj_xml = lxml.etree.tostring(obj.toxml()).decode()
        with open(f"{obj.__class__.__name__.lower()}.xml", "wb") as _out:
            _out.write(lxml.etree.tostring(obj_xml, pretty_print=True,))

        response = requests.post(
            url,
            files={
                # "SUBMISSION": StringIO(Submission.generate_submission(hold_date=self.hold_date)),
                "SUBMISSION": StringIO(submission_xml),
                # obj.__class__.__name__.upper().replace("SET", ""): StringIO(
                #     lxml.etree.tostring(obj.toxml()).decode()
                # ),
                obj.__class__.__name__.upper().replace("SET", ""): StringIO(lxml.etree.tostring(obj_xml).decode()),
            },
            auth=self.get_auth(),
            timeout=self.timeout,
        )

        response_xml = "\n".join(line for line in response.text.strip().split("\n") if line[:5] != "<?xml")
        with open(f"{obj.__class__.__name__.lower()}_ena_response.xml", "wt") as _out:
            _out.write(response_xml)

        return SubmissionResponse.from_xml(response_xml, obj.__class__)


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
