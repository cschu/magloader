import argparse
import csv

import lxml.etree, lxml.builder

from study import Study


def parse_studies(f):
    with open(f, "rt", encoding="UTF-8") as _in:
        for study_data in csv.DictReader(_in, delimiter="\t"):
            study = Study(**study_data)
            print(study)
            # yield study2xml(study)
            yield study.toxml()


def study2xml(study_obj):
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
                study_title(study_obj.get_title()),
                study_type(
                    existing_study_type="Other",
                    new_study_type=study_obj.new_study_type,),
                study_description(study_obj.get_description()),
            ),
            study_links(
                study_link(
                    url_link(
                        label("SPIRE"),
                        url(study_obj.spire_study_link),
                    )
                ),
                *(
                    study_link(
                        xref_link(
                            db("ENA-SUBMISSION"),
                            id_(xid),
                        )
                    )
                    for xid in study_obj.get_raw_data_projects()
                ),
            ),
            study_attributes(
                study_attribute(
                    tag("study keyword"),
                    value(study_obj.study_keyword),
                )
            ),
            alias=study_obj.study_id,
            center_name=study_obj.center_name,
        )
    )

    doc2 = study_set(
        study(
            descriptor(
                study_title("SPIRE v01 TPA metagenomic analyses (assembly + MAGs) of project spire_study_4"),
                study_type(existing_study_type="Other", new_study_type="Metagenomic assembly"),
                study_description("Third Party Annotations (TPA) derived from PRJEB22997 as part of the SPIRE database v01 where the data is accessible under study spire_study_4. This project bundles data on metagenomic assemblies (using MEGAHIT v1.2.9) and derived metagenome-assembled genomes. Data was processed using the SPIRE pipeline v1.0.0. Please see https://spire.embl.de/study/4 for additional information."),
            ),
            study_links(
                study_link(
                    url_link(
                        label("SPIRE"),
                        url("https://spire.embl.de/study/4"),
                    )
                ),
                study_link(
                    xref_link(
                        db("ENA-SUBMISSION"),
                        id_("PRJEB22997"),
                    )
                ),
            ),
            study_attributes(
                study_attribute(
                    tag("study keyword"),
                    value("TPA:assembly"),
                )
            ),
            alias="spire_study_4zz",
            center_name="SPIRE EMBL Heidelberg",
        )
    )    

    print(lxml.etree.tostring(doc).decode())

    # print(TITLE)
    return doc


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("study", type=str)

    args = ap.parse_args()


    for study_xml in parse_studies(args.study):
        print(study_xml)
    


if __name__ == "__main__":
    main()