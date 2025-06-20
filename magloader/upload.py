import pathlib

from .manifest import Manifest
from .webin import EnaWebinClient
from .workdir import working_directory


def check_assemblies(biosamples, assemblies):
    for biosample in biosamples:
        assembly = assemblies.get(biosample.alias)
        if assembly is None:
            raise ValueError(f"{biosample.alias} does not have an assembly!")
        yield biosample.accession, assembly



def prepare_manifest_files(study_id, assemblies, workdir):
    for biosample_accession, assembly in assemblies:
        assembly_dir = workdir / "assemblies" / assembly.assembly_name
        assembly_done = assembly_dir / "DONE"
        if not assembly_done.is_file():
            assembly_dir.mkdir(parents=True, exist_ok=True,)
            manifest_file = pathlib.Path(assembly_dir / f"{assembly.assembly_name}.manifest.txt")
            if not manifest_file.is_file():
                with open(manifest_file, "wt") as _out:
                    manifest = Manifest.from_assembly(assembly, study_id, biosample_accession)
                    print(manifest.to_str(), file=_out,)
                print(manifest)
                yield manifest_file
                
def process_manifest(manifest_file, user, password, submit=True, run_on_dev_server=False,):
    webin_client = EnaWebinClient(user, password)
    with working_directory(manifest_file.parent):
        is_valid, messages = webin_client.validate(manifest_file, dev=run_on_dev_server,)
        if is_valid and submit:
            ena_id, messages = webin_client.submit(manifest_file, dev=run_on_dev_server,)
            if ena_id:
                pathlib.Path("DONE").touch()
                return ena_id, []
        return None, messages
        
        
        
    


# for biosample in biosamples:
#         assembly = assemblies.get(biosample.alias)
#         if assembly is None:
#             raise ValueError(f"{biosample.alias} does not have an assembly!")
#         assembly_dir = workdir / "assemblies" / assembly.assembly_name
#         assembly_done = assembly_dir / "DONE"
#         if not assembly_done.is_file():
#             assembly_dir.mkdir(parents=True, exist_ok=True,)
#             with working_directory(assembly_dir):
#                 manifest = Manifest.from_assembly(assembly, study_id, biosample.accession)
#                 manifest_file = pathlib.Path(f"{assembly.assembly_name}.manifest.txt")
#                 if not manifest_file.is_file():
#                     with open(manifest_file, "wt") as _out:
#                         print(manifest.to_str(), file=_out,)
#                 print(manifest)

#                 is_valid, messages = webin_client.validate(manifest_file, dev=run_on_dev_server,)
#                 if is_valid:
#                     ena_id, messages = webin_client.submit(manifest_file, dev=run_on_dev_server,)
#                     if ena_id:
#                         messages = []
#                         print("ENA-ID", ena_id,)
#                         pathlib.Path("DONE").touch()

#                 if messages:
#                     print(*messages, sep="\n",)

#                 print("-----------------------------------------------------")