from dataclasses import dataclass
from io import StringIO

import lxml.etree, lxml.builder
import requests


from .submission import SubmissionResponse, SubmissionResponseObject


DESCRIPTION = """
Third Party Annotations (TPA) derived from dataset{multiple_datasets} {raw_data_projects}
as part of the SPIRE database v01,
where the data is accessible under study '{study_name}'.
This project bundles data on metagenomic assemblies
(using {assembler} {assembler_version}) and derived metagenome-assembled genomes.
Data was processed using the {pipeline} {pipeline_version}.
Please see {url} for additional information.""".strip()

TITLE = """
SPIRE v01 TPA metagenomic analyses (assembly & MAGs) of project {study_name}
""".strip()

SPIRE_LINK = """
https://spire.embl.de/study/{study_id}
""".strip()



@dataclass
class Study:
    study_id: str = None
    study_name: str = None
    # title: str = None
    raw_data_projects: str = None
    # spire_study_link: str = None
    # description: str = None
    center_name: str = "EMBL Heidelberg"
    study_keyword: str = "TPA:assembly"
    new_study_type: str = "Metagenomic assembly"
    assembler: str = "MEGAHIT"
    assembler_version: str = "v1.2.9"
    pipeline: str = "SPIRE pipeline"
    pipeline_version: str = "v1.0.0"



    def __post_init__(self):
        # self.study_id = self.title.strip().split(" ")[-1]
        # print(f"{self.study_id=}")
        ...
    def get_spire_link(self):
        return SPIRE_LINK.format(study_id=self.study_id)

    def get_description(self):
        return DESCRIPTION.format(
            raw_data_projects=self.raw_data_projects,
            study_name=self.study_name,
            url=self.get_spire_link(),
            assembler=self.assembler,
            assembler_version=self.assembler_version,
            pipeline=self.pipeline,
            pipeline_version=self.pipeline_version,
            multiple_datasets="s" if "," in self.raw_data_projects else "",
        ).replace("\n", " ")
    def get_title(self):
        # return TITLE.format(study_id=self.study_id)
        return TITLE.format(study_name=self.study_name)
    def get_raw_data_projects(self):
        yield from self.raw_data_projects.strip().split(",")

    def toxml(self):
        maker = lxml.builder.ElementMaker()

        study_set = maker.STUDY_SET
        study = maker.STUDY
        descriptor = maker.DESCRIPTOR
        study_title = maker.STUDY_TITLE
        study_type = maker.STUDY_TYPE
        study_description = maker.STUDY_DESCRIPTION
        study_links = maker.STUDY_LINKS
        study_link = maker.STUDY_LINK
        url_link = maker.URL_LINK
        label = maker.LABEL
        url = maker.URL
        xref_link = maker.XREF_LINK
        db = maker.DB
        id_ = maker.ID
        study_attributes = maker.STUDY_ATTRIBUTES
        study_attribute = maker.STUDY_ATTRIBUTE
        tag = maker.TAG
        value = maker.VALUE

        doc = study_set(
            study(
                descriptor(
                    study_title(self.get_title()),
                    study_type(
                        existing_study_type="Other",
                        new_study_type=self.new_study_type,),
                    study_description(self.get_description()),
                ),
                study_links(
                    study_link(
                        url_link(
                            label("SPIRE"),
                            url(self.get_spire_link()),
                        )
                    ),
                    *(
                        study_link(
                            xref_link(
                                db("ENA-SUBMISSION"),
                                id_(xid),
                            )
                        )
                        for xid in self.get_raw_data_projects()
                    ),
                ),
                study_attributes(
                    study_attribute(
                        tag("study keyword"),
                        value(self.study_keyword),
                    )
                ),
                alias=f"spire_study_{self.study_id}",
                center_name=self.center_name,
            )
        )

        return doc
    
    # def submit(self, user, pw, hold_date=None, dev=True):
    #     # requests.post(url, files={"SUBMISSION": open("submission.xml", "rb"), "STUDY": open("study3.xml", "rb")}, auth=(webin, pw))
    #     url = f"https://www{('', 'dev')[dev]}.ebi.ac.uk/ena/submit/drop-box/submit/"
    #     response = requests.post(
    #         url,
    #         files={
    #             "SUBMISSION": StringIO(generate_submission(hold_date=hold_date)),
    #             "STUDY": StringIO(self.toxml()),
    #         },
    #         auth=(user, pw,),
    #         timeout=60,
    #     )

    #     return parse_submission_response(response)
        
    @staticmethod
    def parse_submission_response(response):

        study = response.find("STUDY")
        if study is not None:
            ext_id = study.find("EXT_ID")
            yield SubmissionResponseObject(
                object_type="study",
                alias=study.attrib.get("alias"),
                status=study.attrib.get("status"),
                hold_until=study.attrib.get("holdUntilDate"),
                accession=study.attrib.get("accession"),
                ext_accession=ext_id.attrib.get("accession") if ext_id is not None else None,
            )


        # xml = "\n".join(line for line in response.text.strip().split("\n") if line[:5] != "<?xml")
        # tree = lxml.etree.fromstring(xml)

        # d = {
        #     "success": tree.attrib.get("success", "false").lower() != "false",
        #     "receipt_date": tree.attrib.get("receiptDate"),
        #     "objects": [],
        # }
        
        # study = tree.find("STUDY")
        # if study is not None:
        #     ext_id = study.find("EXT_ID")
        #     study_obj = SubmissionResponseObject(
        #         object_type="study",
        #         alias=study.attrib.get("alias"),
        #         status=study.attrib.get("status"),
        #         hold_until=study.attrib.get("holdUntilDate"),
        #         accession=study.attrib.get("accession"),
        #         ext_accession=ext_id.attrib.get("accession") if ext_id is not None else None,
        #     )
        #     d["objects"].append(study_obj)
        #     # d["alias"] = study.attrib.get("alias")
        #     # d["status"] = study.attrib.get("status")
        #     # d["hold_until"] = study.attrib.get("holdUntilDate")
        #     # d["accession"] = study.attrib.get("accession")
        #     # ext_id = study.find("EXT_ID")
        #     # if ext_id is not None:
        #     #     d["ext_accession"] = ext_id.attrib.get("accession")

        # submission = tree.find("SUBMISSION")
        # if submission is not None:
        #     d["submission_accession"] = submission.attrib.get("accession")
        #     d["submission_alias"] = submission.attrib.get("alias")

        # messages = tree.find("MESSAGES")
        # if messages is not None:
        #     d["messages"] = [(m.tag, m.text) for m in messages.getchildren()]
        

        # return SubmissionResponse(**d)


    

"""
<RECEIPT receiptDate="2024-10-30T13:20:16.535Z" submissionFile="SUBMISSION" success="false">
     <STUDY alias="spire_study_4" status="PRIVATE" holdUntilDate="2024-10-31Z"/>
     <SUBMISSION alias="SUBMISSION-30-10-2024-13:20:16:527"/>
     <MESSAGES>
          <ERROR>In study, alias: "spire_study_4". The object being added already exists in the submission account with accession: "ERP165656".</ERROR>
          <INFO>This submission is a TEST submission and will be discarded within 24 hours</INFO>
     </MESSAGES>
     <ACTIONS>ADD</ACTIONS>
     <ACTIONS>HOLD</ACTIONS>
</RECEIPT>



<RECEIPT receiptDate="2024-10-30T13:48:47.796Z" submissionFile="SUBMISSION" success="true">
     <STUDY accession="ERP165660" alias="spire_study_4.zzzzzzz" status="PRIVATE" holdUntilDate="2024-10-31Z">
          <EXT_ID accession="PRJEB81875" type="Project"/>
     </STUDY>
     <SUBMISSION accession="ERA30918678" alias="SUBMISSION-30-10-2024-13:48:47:614"/>
     <MESSAGES>
          <INFO>All objects in this submission are set to private status (HOLD).</INFO>
          <INFO>This submission is a TEST submission and will be discarded within 24 hours</INFO>
     </MESSAGES>
     <ACTIONS>ADD</ACTIONS>
     <ACTIONS>HOLD</ACTIONS>
</RECEIPT>
"""

