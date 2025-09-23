from dataclasses import dataclass


DESCRIPTION = """
SPIRE v01 primary metagenome assembly for {accessions}. For more details see https://spire.embl.de/spire/v1/genome/{sample_id}
""".strip()

MAG_DESCRIPTION = """
SPIRE v01 Metagenome-Assembled Genome {spire_genome_id}. For more details see https://spire.embl.de/spire/v1/genome/{spire_genome_id}.
""".strip()

@dataclass
class Manifest:
    study: str = None
    sample: str = None
    assemblyname: str = None
    assembly_type: str = "primary metagenome"
    program: str = None
    platform: str = "Illumina"
    moleculetype: str = "genomic DNA"
    fasta: str = None
    coverage: int = None
    description: str = ""

    def to_str(self):
        return "\n".join(
            f"{k.upper()}   {v}"
            for k, v in self.__dict__.items()
            if v
        )

    @classmethod
    def from_assembly(cls, assembly, ena_study, ena_sample, mags=False,):
        # spire_ena_project_id	sample_id	assembly_name	assembly_type	program	description	file_path
        if mags:
            description = MAG_DESCRIPTION.format(spire_genome_id=assembly.assembly_name,)
        else:
            description = DESCRIPTION.format(accessions=ena_sample, sample_id=assembly.sample_id,),
        return cls(
            study=ena_study,
            sample=ena_sample,
            assemblyname=assembly.assembly_name,
            assembly_type=assembly.assembly_type,
            program=assembly.program,
            fasta=assembly.file_path,
            coverage=assembly.coverage,
            description=description,
        )
