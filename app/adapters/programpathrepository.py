from abc import ABC, abstractmethod
import os
from pathlib import Path
from typing import Dict, List, Union, Type

from app.internal.httpresponse import HTTPResponse
from app.internal.settings import Settings
from app.models.program import Program


class AbstractProgramPathRepository(ABC):
    """ """

    @classmethod
    @abstractmethod
    async def list_programs(cls) -> Union[List[Program], HTTPResponse]:
        pass


class PEMAWSProgramPathRepository(AbstractProgramPathRepository):
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
    ) -> Union[List[Program], HTTPResponse]:
        programs: List[Program] = []
        if not os.path.isdir(programPath):
            return HTTPResponse(
                code=500,
                detail=f"program path not found: {cls.ROOT_PROGRAM_PATH}",
            )
        versions = os.listdir(programPath)
        for i, v in enumerate(versions):
            versionPath = programPath.joinpath(v)
            if not os.path.isdir(versionPath):
                continue
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
                    executablePath=str(versionPath.joinpath(execFiles[0])),
                )
            )
        return programs

    @classmethod
    async def __list_newave(cls) -> Union[List[Program], HTTPResponse]:
        return await PEMAWSProgramPathRepository.__list_program(
            "NW", cls.NEWAVE_PATH, "NEWAVE", ["N_PROC"], "mpi_newave"
        )

    @classmethod
    async def __list_decomp(cls) -> Union[List[Program], HTTPResponse]:
        return await PEMAWSProgramPathRepository.__list_program(
            "DC", cls.DECOMP_PATH, "DECOMP", ["N_PROC"], "mpi_decomp"
        )

    @classmethod
    async def list_programs(cls) -> Union[List[Program], HTTPResponse]:
        newave = await cls.__list_newave()
        if isinstance(newave, HTTPResponse):
            return newave
        decomp = await cls.__list_decomp()
        if isinstance(decomp, HTTPResponse):
            return decomp
        return newave + decomp


class TuberProgramPathRepository(AbstractProgramPathRepository):
    """
    Implements the installation patter for managed programs
    in the PEM AWS setup using the 'tuber' CLI tool.

    The main premises are:
        - A root path for all the managed programs
        - A subfolder with each program name
        - A second level of subfolders with the versions
        - Each version containing an mpi_program*.job file
    """

    ROOT_PROGRAM_PATH = Path("/home/pem/versoes")
    NEWAVE_PATH = ROOT_PROGRAM_PATH.joinpath("NEWAVE")
    DECOMP_PATH = ROOT_PROGRAM_PATH.joinpath("DECOMP")
    DESSEM_PATH = Path("/home/SW/dessem")
    NEWAVE_TUBER_JOB = "/home/pem/rotinas/tuber/jobs/mpi_newave.job"
    DECOMP_TUBER_JOB = "/home/pem/rotinas/tuber/jobs/mpi_decomp.job"
    DESSEM_TUBER_JOB = "/home/ESTUDO/PEM/git/tuber/jobs/dessem.sh"

    @classmethod
    async def __list_program(
        cls,
        idPrefix: str,
        programPath: Path,
        name: str,
        args: List[str],
        execPattern: str,
    ) -> Union[List[Program], HTTPResponse]:
        programs: List[Program] = []
        if not os.path.isdir(programPath):
            return []
        versions = os.listdir(programPath)
        for i, v in enumerate(versions):
            versionPath = programPath.joinpath(v)
            if not os.path.isdir(str(versionPath)):
                continue
            execTuber = execPattern + " " + v[1:]
            programs.append(
                Program(
                    programId=f"{idPrefix}{i}",
                    name=name,
                    clusterId=Settings.clusterId,
                    version=v,
                    installationDirectory=str(versionPath),
                    isManaged=True,
                    args=args,
                    executablePath=str(execTuber),
                )
            )
        return programs

    @classmethod
    async def __list_executables(
        cls,
        idPrefix: str,
        programPath: Path,
        name: str,
        args: List[str],
        execPattern: str,
    ) -> Union[List[Program], HTTPResponse]:
        programs: List[Program] = []
        if not os.path.isdir(programPath):
            return []
        versions = os.listdir(programPath)
        for i, v in enumerate(versions):
            versionPath = programPath.joinpath(v)
            if not os.path.isfile(str(versionPath)):
                continue
            namePattern = name.lower() + "_"
            if namePattern not in v:
                continue
            versionName = v.split(namePattern)[1]
            execTuber = execPattern + " " + versionName
            programs.append(
                Program(
                    programId=f"{idPrefix}{i}",
                    name=name,
                    clusterId=Settings.clusterId,
                    version=versionName,
                    installationDirectory=str(versionPath),
                    isManaged=True,
                    args=args,
                    executablePath=execTuber,  # TODO - fix
                )
            )
        return programs

    @classmethod
    async def __list_newave(cls) -> Union[List[Program], HTTPResponse]:
        return await TuberProgramPathRepository.__list_program(
            "NW", cls.NEWAVE_PATH, "NEWAVE", ["N_PROC"], cls.NEWAVE_TUBER_JOB
        )

    @classmethod
    async def __list_decomp(cls) -> Union[List[Program], HTTPResponse]:
        return await TuberProgramPathRepository.__list_program(
            "DC", cls.DECOMP_PATH, "DECOMP", ["N_PROC"], cls.DECOMP_TUBER_JOB
        )

    @classmethod
    async def __list_dessem(cls) -> Union[List[Program], HTTPResponse]:
        return await TuberProgramPathRepository.__list_executables(
            "DS", cls.DESSEM_PATH, "DESSEM", [], cls.DESSEM_TUBER_JOB
        )

    @classmethod
    async def list_programs(cls) -> Union[List[Program], HTTPResponse]:
        newave = await cls.__list_newave()
        if isinstance(newave, HTTPResponse):
            return newave
        decomp = await cls.__list_decomp()
        if isinstance(decomp, HTTPResponse):
            return decomp
        dessem = await cls.__list_dessem()
        if isinstance(dessem, HTTPResponse):
            return dessem
        return newave + decomp + dessem


class TestProgramPathRepository(AbstractProgramPathRepository):
    """ """

    @classmethod
    async def list_programs(cls) -> Union[List[Program], HTTPResponse]:
        return [
            Program(
                programId="NW1",
                name="NEWAVE",
                clusterId="0",
                version="29",
                installationDirectory="/tmp/NEWAVE/v29",
                isManaged=True,
                executablePath="/tmp/NEWAVE/v29/mpi_newave",
                args=["N_PROC"],
            ),
            Program(
                programId="DC1",
                name="DECOMP",
                clusterId="0",
                version="29",
                installationDirectory="/tmp/DECOMP/v32",
                isManaged=True,
                executablePath="/tmp/DECOMP/v32/mpi_decomp",
                args=["N_PROC"],
            ),
        ]


SUPPORTED_PATHS: Dict[str, Type[AbstractProgramPathRepository]] = {
    "PEMAWS": PEMAWSProgramPathRepository,
    "TUBER": TuberProgramPathRepository,
    "TEST": TestProgramPathRepository,
}
DEFAULT = PEMAWSProgramPathRepository


def factory(kind: str) -> Type[AbstractProgramPathRepository]:
    return SUPPORTED_PATHS.get(kind, DEFAULT)
