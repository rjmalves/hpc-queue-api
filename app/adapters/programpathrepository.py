from abc import ABC, abstractmethod
import os
from pathlib import Path
from typing import Dict, List

from app.internal.settings import Settings
from app.models.program import Program


class AbstractProgramPathRepository(ABC):
    """ """

    @classmethod
    @abstractmethod
    async def list_programs() -> List[Program]:
        pass


class PEMAWSProgramPathRepository(ABC):
    """
    Implements the installation patter for managed programs
    in the PEM AWS setup.

    The main premises are:
        - A root path for all the managed programs
        - A subfolder with each program name
        - A second level of subfolders with the versions
        - Each version containing an mpi_program*.job file
    """

    ROOT_PROGRAM_PATH = Path("/home/pem/versoes")
    NEWAVE_PATH = ROOT_PROGRAM_PATH.joinpath("NEWAVE")
    DECOMP_PATH = ROOT_PROGRAM_PATH.joinpath("DECOMP")

    @classmethod
    async def __list_program(
        cls,
        idPrefix: str,
        programPath: Path,
        name: str,
        args: List[str],
        execPattern: str,
    ) -> List[Program]:
        programs: List[Program] = []
        versions = os.listdir(programPath)
        for i, v in enumerate(versions):
            versionPath = programPath.joinpath(v)
            execFiles = [
                f for f in os.listdir(versionPath) if execPattern in f
            ]
            if len(execFiles) != 1:
                continue
            programs.append(
                Program(
                    programId=f"{idPrefix}{i}",
                    name=name,
                    clusterId=Settings.clusterId,
                    version=v,
                    installationDirectory=str(versionPath),
                    isManaged=True,
                    args=args,
                    executablePath=execFiles[0],
                )
            )
        return programs

    @classmethod
    async def __list_newave(cls) -> List[Program]:
        return await PEMAWSProgramPathRepository.__list_program(
            "NW", cls.NEWAVE_PATH, "NEWAVE", ["N_PROC"], "mpi_newave"
        )

    @classmethod
    async def __list_decomp(cls) -> List[Program]:
        return await PEMAWSProgramPathRepository.__list_program(
            "DC", cls.DECOMP_PATH, "DECOMP", ["N_PROC"], "mpi_decomp"
        )

    @classmethod
    async def list_programs(cls) -> List[Program]:
        newave = await cls.__list_newave()
        decomp = await cls.__list_decomp()
        return newave + decomp


SUPPORTED_PATHS: Dict[str, AbstractProgramPathRepository] = {
    "PEMAWS": PEMAWSProgramPathRepository
}
DEFAULT = PEMAWSProgramPathRepository


def factory(kind: str) -> AbstractProgramPathRepository:
    s = SUPPORTED_PATHS.get(kind)
    if s is None:
        return DEFAULT
    return s
