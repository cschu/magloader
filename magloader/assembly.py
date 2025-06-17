from dataclasses import dataclass

from .sample import Sample

# {
# 				"sample_id": "176",
# 				"biosamples": "SAMEA104408592",
# 				"file_path": "/g/scb/bork/data/spire/studies/4/psa_megahit/assemblies/SAMEA104408592-assembled.fa.gz",
# 				"program": "megahit v1.2.9",
# 				"coverage": "15.8087"
# 			},


@dataclass
class Assembly:
    # spire_ena_project_id	sample_id	assembly_name	assembly_type	program	description	file_path
    spire_ena_project_id: str = None
    sample_id: str = None
    assembly_name: str = None
    assembly_type: str = "primary metagenome"
    program: str = None
    # description: str = None
    file_path: str = None
    coverage: str = None
    biosamples: str = None
    program_version: str = None
    
    def __post_init__(self):
        if self.program_version:
            self.program = f"{self.program} v{self.program_version}"
        if not self.assembly_name:
            self.assembly_name = f"spire_assembly_{self.sample_id}"
        
    
    def get_sample(self):
        print(self)
        return Sample(
            spire_ena_project_id=self.spire_ena_project_id,
            sample_id=self.sample_id,
            biosamples=self.biosamples,
            # description=self.description,
        )
