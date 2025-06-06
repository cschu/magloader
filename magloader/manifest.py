from dataclasses import dataclass


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

	def to_str(self):
		return "\n".join(
			f"{k.upper()}   {v}"	
			for k, v in self.__dict__.items()
		)
	
	@classmethod
	def from_assembly(cls, assembly, ena_study, ena_sample, coverage=10,):
		# spire_ena_project_id	sample_id	assembly_name	assembly_type	program	description	file_path
		return cls(
			study=ena_study,
			sample=ena_sample,
			assemblyname=assembly.assembly_name,
			assembly_type=assembly.assembly_type,
			program=assembly.program,
			fasta=assembly.file_path,
			coverage=coverage,
		)





# STUDY   ERP173187
# SAMPLE   ERS31594040
# ASSEMBLYNAME   TODO
# ASSEMBLY_TYPE   primary metagenome
# PROGRAM   megahit_v1.2.9
# PLATFORM   Illumina
# MOLECULETYPE   genomic DNA
# FASTA   primary_metagenome.fasta.gz
# COVERAGE   10