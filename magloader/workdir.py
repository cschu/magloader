import contextlib
import os
import pathlib


@contextlib.contextmanager
def working_directory(path):
    # https://stackoverflow.com/questions/41742317/how-can-i-change-directory-with-python-pathlib
    """Changes working directory and returns to previous on exit."""
    prev_cwd = pathlib.Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)