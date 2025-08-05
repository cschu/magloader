import re

from abc import ABC, abstractmethod
from dataclasses import dataclass
from textwrap import dedent

import lxml.builder

from .submission import SubmissionResponseObject



class Study(ABC):
    def __init__(
        self,
        study_id: str = None,
        study_name: str = None,
        raw_data_projects: str = None,
        center_name: str = None,
        study_keyword: str = None,
        new_study_type: str = None,
        assembler: str = None,
        assembler_version: str = None,
        pipeline: str = None,
        pipeline_version: str = None,
    ):
        self.study_id = study_id
        self.study_name = study_name
        self.raw_data_projects = raw_data_projects
        self.center_name = center_name
        self.study_keyword = study_keyword
        self.new_study_type = new_study_type
        self.assembler = assembler
        self.assembler_version = assembler_version
        self.pipeline = pipeline
        self.pipeline_version = pipeline_version

    def get_base(self):
        # return self.__class__.__name__
        return self.__class__.__mro__[:-2][-1]

    def get_raw_data_projects(self):
        yield from self.raw_data_projects.strip().split(",")
    
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

class SpireStudy(Study):
    def __init__(
        self,
        study_id: str = None,
        raw_data_projects: str = None,
        assembler: str = None,
        assembler_version: str = None,
        pipeline_version: str = None,
    ):
        super().__init__(
            study_id = study_id,
            study_name = f"spire_study_{study_id}",
            raw_data_projects = raw_data_projects,
            center_name = "EMBL Heidelberg",
            study_keyword = "TPA:assembly",
            new_study_type = "Metagenomic assembly",
            assembler = assembler,
            assembler_version = assembler_version,
            pipeline = "SPIRE pipeline",
            pipeline_version = pipeline_version,
        )

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
                                db("BIOPROJECT"),
                                id_(xid),
                            )
                        )
                        for xid in self.get_raw_data_projects()
                        if xid[:3] == "PRJ"
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
    
    @abstractmethod
    def get_spire_link(self):
        ...
    
    @abstractmethod
    def get_description(self):
        ...
    
    @abstractmethod
    def get_title(self):
        ...

class SpireV1Study(SpireStudy):
    def __init__(
        self,
        study_id: str = None,
        raw_data_projects: str = None,
    ):
        super().__init__(
            study_id = study_id,
            raw_data_projects = raw_data_projects,
            assembler = "MEGAHIT",
            assembler_version = "v1.2.9",
            pipeline_version = "v1.0.0",
        )

    def get_spire_link(self):
        return f"https://spire.embl.de/spire/v1/study/{self.study_id}"

    def get_title(self):
        return f"SPIRE v01 TPA metagenomic analyses (assembly & MAGs) of project {self.study_name}"
    
    def get_description(self):
        return ""


class SpireV1StudyEna(SpireV1Study):
    def __init__(
        self,
        study_id: str = None,
        raw_data_projects: str = None,
    ):
        super().__init__(
            study_id = study_id,
            raw_data_projects = raw_data_projects,
        )

    def get_spire_link(self):
        return super().get_spire_link()
    
    def get_title(self):
        return super().get_title()
    
    def get_description(self):
        description = f"""
            Third Party Annotations (TPA) derived from dataset(s) {self.raw_data_projects}
            as part of the SPIRE database v01.
            This project bundles data on metagenomic assemblies
            (using {self.assembler} {self.assembler_version}) and derived metagenome-assembled genomes.
            Data was processed using the {self.pipeline} {self.pipeline_version}.
            The project is accessible under {self.get_spire_link()}.
            """
        
        return re.sub(r' +', " ", dedent(description.strip()).replace("\n", " "))


class SpireV1StudyMgRast(SpireV1Study):
    def __init__(
        self,
        study_id: str = None,
        raw_data_projects: str = None,
    ):
        super().__init__(
            study_id = study_id,
            raw_data_projects = raw_data_projects,
        )

    def get_spire_link(self):
        return super().get_spire_link()
    
    def get_title(self):
        return super().get_title()
    
    def get_description(self):
        description = f"""
            Third Party Annotations (TPA) derived from MG-RAST data {self.raw_data_projects}
            as part of the SPIRE database v01.
            This project bundles data on metagenomic assemblies
            (using {self.assembler} {self.assembler_version}) and derived metagenome-assembled genomes.
            Data was processed using the {self.pipeline} {self.pipeline_version}.
            The project is accessible under {self.get_spire_link()}.
            """
        
        return re.sub(r' +', " ", dedent(description.strip()).replace("\n", " "))

class SpireV1StudyMetaSub(SpireV1Study):
    def __init__(
        self,
        study_id: str = None,
        raw_data_projects: str = None,
    ):
        super().__init__(
            study_id = study_id,
            raw_data_projects = raw_data_projects,
        )

    def get_spire_link(self):
        return super().get_spire_link()
    
    def get_title(self):
        return super().get_title()
    
    def get_description(self):
        description = f"""
            Third Party Annotations (TPA) derived from MetaSUB consortium data
            as part of the SPIRE database v01.            
            Original data described by Danko et al (2020, https://www.biorxiv.org/content/10.1101/724526v5.full) 
            and accessed as described therein via https://github.com/MetaSUB/metasub_utils .
            This project bundles data on metagenomic assemblies
            (using {self.assembler} {self.assembler_version}) and derived metagenome-assembled genomes.
            Data was processed using the {self.pipeline} {self.pipeline_version}.
            The project is accessible under {self.get_spire_link()}.
            """
        
        return re.sub(r' +', " ", dedent(description.strip()).replace("\n", " "))
    
class SpireV1StudyInternal(SpireV1Study):
    def __init__(
        self,
        study_id: str = None,
        raw_data_projects: str = None,
    ):
        super().__init__(
            study_id = study_id,
            raw_data_projects = raw_data_projects,
        )

    def get_spire_link(self):
        return super().get_spire_link()
    
    def get_title(self):
        return super().get_title()
    
    def get_description(self):
        description = f"""
            Third Party Annotations (TPA) as part of the SPIRE database v01.            
            This project bundles data on metagenomic assemblies
            (using {self.assembler} {self.assembler_version}) and derived metagenome-assembled genomes.
            Data was processed using the {self.pipeline} {self.pipeline_version}.
            The project is accessible under {self.get_spire_link()}.
            """
        
        return re.sub(r' +', " ", dedent(description.strip()).replace("\n", " "))



# @dataclass
# class Study:
#     study_id: str = None
#     study_name: str = None
#     raw_data_projects: str = None
#     center_name: str = "EMBL Heidelberg"
#     study_keyword: str = "TPA:assembly"
#     new_study_type: str = "Metagenomic assembly"
#     assembler: str = None
#     assembler_version: str = None
#     pipeline: str = "SPIRE pipeline"
#     pipeline_version: str = None

#     def __post_init__(self):
#         self.study_name = f"spire_study_{self.study_id}"

#     @abstractmethod
#     def get_spire_link(self):
#         return SPIRE_LINK.format(study_id=self.study_id)

#     def get_description(self):
#         return DESCRIPTION.format(
#             raw_data_projects=self.raw_data_projects,
#             study_name=self.study_name,
#             url=self.get_spire_link(),
#             assembler=self.assembler,
#             assembler_version=self.assembler_version,
#             pipeline=self.pipeline,
#             pipeline_version=self.pipeline_version,
#             # multiple_datasets="s" if "," in self.raw_data_projects else "",
#         ).replace("\n", " ")
#     def get_title(self):
#         return TITLE.format(study_name=self.study_name)
#     def get_raw_data_projects(self):
#         yield from self.raw_data_projects.strip().split(",")

#     def toxml(self):
#         maker = lxml.builder.ElementMaker()

#         study_set = maker.STUDY_SET
#         study = maker.STUDY
#         descriptor = maker.DESCRIPTOR
#         study_title = maker.STUDY_TITLE
#         study_type = maker.STUDY_TYPE
#         study_description = maker.STUDY_DESCRIPTION
#         study_links = maker.STUDY_LINKS
#         study_link = maker.STUDY_LINK
#         url_link = maker.URL_LINK
#         label = maker.LABEL
#         url = maker.URL
#         xref_link = maker.XREF_LINK
#         db = maker.DB
#         id_ = maker.ID
#         study_attributes = maker.STUDY_ATTRIBUTES
#         study_attribute = maker.STUDY_ATTRIBUTE
#         tag = maker.TAG
#         value = maker.VALUE

#         doc = study_set(
#             study(
#                 descriptor(
#                     study_title(self.get_title()),
#                     study_type(
#                         existing_study_type="Other",
#                         new_study_type=self.new_study_type,),
#                     study_description(self.get_description()),
#                 ),
#                 study_links(
#                     study_link(
#                         url_link(
#                             label("SPIRE"),
#                             url(self.get_spire_link()),
#                         )
#                     ),
#                     *(
#                         study_link(
#                             xref_link(
#                                 # db("ENA-SUBMISSION"),
#                                 db("BIOPROJECT"),
#                                 id_(xid),
#                             )
#                         )
#                         for xid in self.get_raw_data_projects()
#                         if xid[:3] == "PRJ"
#                     ),
#                 ),
#                 study_attributes(
#                     study_attribute(
#                         tag("study keyword"),
#                         value(self.study_keyword),
#                     )
#                 ),
#                 alias=f"spire_study_{self.study_id}",
#                 center_name=self.center_name,
#             )
#         )

#         return doc

#     @staticmethod
#     def parse_submission_response(response):

#         study = response.find("STUDY")
#         if study is not None:
#             ext_id = study.find("EXT_ID")
#             yield SubmissionResponseObject(
#                 object_type="study",
#                 alias=study.attrib.get("alias"),
#                 status=study.attrib.get("status"),
#                 hold_until=study.attrib.get("holdUntilDate"),
#                 accession=study.attrib.get("accession"),
#                 ext_accession=ext_id.attrib.get("accession") if ext_id is not None else None,
#             )
STUDY_TYPES = {
    "ena": SpireV1StudyEna,
    "mg-rast": SpireV1StudyMgRast,
    "metasub": SpireV1StudyMetaSub,
    "internal": SpireV1StudyInternal,
}
