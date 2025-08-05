import re

from abc import ABC, abstractmethod
from dataclasses import dataclass

import lxml.builder

from .submission import SubmissionResponseObject


class Sample(ABC):
    def __init__(
        self,
        spire_ena_project_id: str = None,
        sample_id: str = None,
        biosamples: str = None,
        taxon_id: str = "256318",
        database: str = "SPIRE",
        db_version: str = "v01",
    ):
        self.spire_ena_project_id = spire_ena_project_id
        self.sample_id = sample_id
        self.biosamples = biosamples
        self.taxon_id = taxon_id
        self.database = database
        self.db_version = db_version

        self.attributes = {
            "collection date": "missing: third party data",
            "geographic location (country and/or sea)": "missing: third party data",
        }

    def get_base(self):
        return self.__class__.__mro__[:-2][-1]
    
    def get_biosamples(self):
        yield from self.biosamples.strip().split(";")

    def get_description(self):
        return f"{self.database} {self.db_version} sample. This is a virtual sample, derived from {self.biosamples}."
    
    def get_title(self):
        return f"{self.database} {self.db_version} sample spire_sample_{self.sample_id}."
    
    @staticmethod
    def parse_submission_response(response):

        failed_samples = {}

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

            if d["accession"] is None:
                failed_samples[d["alias"]] = d
            else:
                yield SubmissionResponseObject(**d)

        if failed_samples:
            messages = response.find("MESSAGES")
            if messages is not None:
                for msg in messages.getchildren():
                    # <ERROR>In sample, alias: "spire_sample_98834". The object being added already exists in the submission account with accession: "ERS25233782".</ERROR>
                    match = re.search(
                        r'In sample, alias: "(.+)"\. The object being added already exists in the submission account with accession: "(.+)"\.',
                        msg.text
                    )
                    if match:
                        alias, accession = match.group(1), match.group(2)
                        d = failed_samples.get(alias)
                        if d is not None:
                            d["accession"] = accession
                            yield SubmissionResponseObject(**d)

    def toxml(self):
        maker = lxml.builder.ElementMaker()

        # sample_attribute = maker.SAMPLE_ATTRIBUTE
        # tag = maker.TAG
        # value = maker.VALUE
        sample_link = maker.SAMPLE_LINK
        xref_link = maker.XREF_LINK
        db = maker.DB
        id_ = maker.ID

        # attributes = [
        #     sample_attribute(
        #         tag(k), value(v)
        #     )
        #     for k, v in self.attributes
        # ]
        
        doc = maker.SAMPLE(
            maker.TITLE(self.get_title()),
            maker.SAMPLE_NAME(
                maker.TAXON_ID(self.taxon_id),
            ),
            maker.DESCRIPTION(self.get_description()),
            maker.SAMPLE_LINKS(
                *(
                    sample_link(
                        xref_link(
                            db("BIOSAMPLE"), id_(bs)
                        )
                    )
                    for bs in self.get_biosamples()
                    if bs[:3] == "SAM"
                ),
                *(
                    sample_link(
                        xref_link(
                            db("MG-RAST"), id_(mgs)
                        )
                    )
                    for mgs in self.get_biosamples()
                    if mgs[:3] == "mgp"
                ),
                sample_link(
                    xref_link(
                        db("BIOPROJECT"), id_(self.spire_ena_project_id)
                    )
                ),
            ),
            maker.SAMPLE_ATTRIBUTES(
                *(
                    maker.SAMPLE_ATTRIBUTE(
                        maker.TAG(k), maker.VALUE(v)
                    )
                    for k, v in self.attributes.items()
                )
            ),
            alias=f"spire_sample_{self.sample_id}",
        )

        return doc


# class MagSample(Sample):
#     def __init__(
#         self,
#         spire_ena_project_id: str = None,
#         sample_id: str = None,
#         biosamples: str = None,
#         taxon_id: str = "256318",
#         database: str = "SPIRE",
#         db_version: str = "v01",
#     ):
#         super().__init__(
#             spire_ena_project_id=spire_ena_project_id,
#             sample_id=sample_id,
#             biosamples=biosamples,
#             taxon_id=taxon_id,
#             database=database,
#             db_version=db_version,
#         )