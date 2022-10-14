from pydantic import BaseModel
from typing import Optional, List


class Program(BaseModel):
    """
    Class for defining an available program in the
    HPC cluster, with details about its execution.
    """

    programId: str
    name: str
    clusterId: str
    version: str
    installationDirectory: str
    isManaged: bool
    executablePath: str
    args: Optional[List[str]]
