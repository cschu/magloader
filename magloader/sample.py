from dataclasses import dataclass

import lxml.builder

from .submission import SubmissionResponseObject


DESCRIPTION = "SPIRE v01 sample. This is a virtual sample, derived from {sample_list}."

TITLE = "SPIRE v01 sample spire_sample_{sample_id}."

class SampleSet:
    def __init__(self):
        self.samples = []
    def toxml(self):
        maker = lxml.builder.ElementMaker()
        sample_set = maker.SAMPLE_SET

        doc = sample_set(
            *(
                sample.toxml()
                for sample in self.samples
            )
        )

        return doc

    @staticmethod
    def parse_submission_response(response):
        yield from Sample.parse_submission_response(response)



@dataclass
class Sample:
    # spire_ena_project_id	sample_id	assembly_name	assembly_type	program	description	file_path
    spire_ena_project_id: str = None
    sample_id: str = None
    # description: str = None
    biosamples: str = None

    def __post_init__(self):
        # self.biosamples = self.description.split(" ")[-1].strip(".")
        ...

    def get_description(self):
        return DESCRIPTION.format(sample_list=self.biosamples)

    def get_title(self):
        return TITLE.format(sample_id=self.sample_id)

    def get_biosamples(self):
        yield from self.biosamples.strip().split(";")

    def get_taxon_id(self):
        return "256318"

    def toxml(self):
        maker = lxml.builder.ElementMaker()

        # sample_set = maker.SAMPLE_SET
        sample = maker.SAMPLE
        title = maker.TITLE
        sample_name = maker.SAMPLE_NAME
        taxon_id = maker.TAXON_ID
        description = maker.DESCRIPTION
        sample_links = maker.SAMPLE_LINKS
        sample_link = maker.SAMPLE_LINK
        xref_link = maker.XREF_LINK
        db = maker.DB
        id_ = maker.ID
        sample_attributes = maker.SAMPLE_ATTRIBUTES
        sample_attribute = maker.SAMPLE_ATTRIBUTE
        tag = maker.TAG
        value = maker.VALUE

        attributes = [
            sample_attribute(
                tag("collection date"), value("missing: third party data",)
            ),
            sample_attribute(
                tag("geographic location (country and/or sea)"), value("missing: third party data",)
            ),
        ]

        doc = sample(
            title(self.get_title()),
            sample_name(
                taxon_id(self.get_taxon_id()),
            ),
            description(self.get_description()),
            sample_links(
                *(
                    sample_link(
                        xref_link(
                            db("BIOSAMPLE"), id_(bs)
                        )
                    )
                    for bs in self.get_biosamples()
                ),
                sample_link(
                    xref_link(
                        db("BIOPROJECT"), id_(self.spire_ena_project_id)
                    )
                ),
            ),
            sample_attributes(
                *attributes
            ),
            alias=f"spire_sample_{self.sample_id}",
        )

        return doc

    @staticmethod
    def parse_submission_response(response):

        for sample in response.findall("SAMPLE"):

            d = {
                "object_type": "sample",
                "alias": sample.attrib.get("alias"),
                "status": sample.attrib.get("status"),
                "hold_until": sample.attrib.get("holdUntilDate"),
                "accession": sample.attrib.get("accession"),
            }

            ext_id = sample.find("EXT_ID")
            d["ext_accession"] = ext_id.attrib.get("accession") if ext_id is not None else None
            yield SubmissionResponseObject(**d)
