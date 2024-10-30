from dataclasses import dataclass

import lxml.etree, lxml.builder


DESCRIPTION = """
Third Party Annotations (TPA) derived from {raw_data_projects}
as part of the SPIRE database v01 where the data
is accessible under study {study_id}.
This project bundles data on metagenomic assemblies
(using MEGAHIT v1.2.9) and derived metagenome-assembled genomes.
Data was processed using the SPIRE pipeline v1.0.0.
Please see {url} for additional information.""".strip()

TITLE = """
SPIRE v01 TPA metagenomic analyses (assembly & MAGs) of project {study_id}
""".strip()



@dataclass
class Study:
    study_id: str = None
    title: str = None
    raw_data_projects: str = None
    spire_study_link: str = None
    description: str = None
    center_name: str = None
    study_keyword: str = None
    new_study_type: str = None

    def __post_init__(self):
        self.study_id = self.title.strip().split(" ")[-1]
        print(f"{self.study_id=}")

    def get_description(self):
        return DESCRIPTION.format(
            raw_data_projects=self.raw_data_projects,
            study_id=self.study_id,
            url=self.spire_study_link,
        ).replace("\n", " ")
    def get_title(self):
        return TITLE.format(study_id=self.study_id)
    def get_raw_data_projects(self):
        yield from self.raw_data_projects.strip().split(";")

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
                            url(self.spire_study_link),
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
                alias=self.study_id,
                center_name=self.center_name,
            )
        )

        return lxml.etree.tostring(doc).decode()