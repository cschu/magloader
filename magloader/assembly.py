from dataclasses import dataclass

from .sample import Sample

@dataclass
class Assembly:
	# spire_ena_project_id	sample_id	assembly_name	assembly_type	program	description	file_path
    spire_ena_project_id: str = None
    sample_id: str = None
    assembly_name: str = None
    assembly_type: str = None
    program: str = None
    description: str = None
    file_path: str = None
    
    def get_sample(self):
        print(self)
        return Sample(
            spire_ena_project_id=self.spire_ena_project_id,
            sample_id=self.sample_id,
            description=self.description,
		)
