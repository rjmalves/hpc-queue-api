from pathlib import Path
import os
import time


class set_directory:
    """
    Directory changing context manager for helping specific cases
    in HPC script executions.
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self.origin = os.getenv("APP_INSTALLDIR")

    def __enter__(self):
        if not self.path.exists():
            time.sleep(5.)
        os.chdir(self.path)

    def __exit__(self, *args, **kwargs):
        os.chdir(self.origin)
