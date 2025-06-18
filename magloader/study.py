from dataclasses import dataclass

import lxml.builder

from .submission import SubmissionResponseObject


DESCRIPTION = """
Third Party Annotations (TPA) derived from dataset{multiple_datasets} {raw_data_projects}
as part of the SPIRE database v01.
This project bundles data on metagenomic assemblies
(using {assembler} {assembler_version}) and derived metagenome-assembled genomes.
Data was processed using the {pipeline} {pipeline_version}.
The project is accessible under {url}.
""".strip()

TITLE = """
SPIRE v01 TPA metagenomic analyses (assembly & MAGs) of project {study_name}
""".strip()

SPIRE_LINK = """
https://spire.embl.de/spire/v1/study/{study_id}
""".strip()



@dataclass
class Study:
    study_id: str = None
    study_name: str = None
    raw_data_projects: str = None
    center_name: str = "EMBL Heidelberg"
    study_keyword: str = "TPA:assembly"
    new_study_type: str = "Metagenomic assembly"
    assembler: str = "MEGAHIT"
    assembler_version: str = "v1.2.9"
    pipeline: str = "SPIRE pipeline"
    pipeline_version: str = "v1.0.0"



    def __post_init__(self):
        self.study_name = f"spire_study_{self.study_id}"

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
