from app.adapters.programpathrepository import factory
from app.models.program import Program
from unittest.mock import MagicMock
import pytest

KB_TO_GB = 1048576
B_TO_GB = 1073741824


@pytest.mark.asyncio
async def test_pemaws_list_programs(mocker):
    repo = factory("PEMAWS")
    mock = MagicMock(return_value=True)
    mocker.patch("os.path.isdir", side_effect=mock)

    def mock_listdir(path):
        if path == repo.NEWAVE_PATH:
            return ["v29"]
        elif path == repo.NEWAVE_PATH.joinpath("v29"):
            return [
                "converteexph29",
                "ConverteNomesArquivos",
                "gerenciamento_PLs29",
                "newave29_L",
                "newdesp29_L",
                "nwlistcf29_L",
                "nwlistop29_L",
                "mpi_newave29.job",
            ]
        elif path == repo.DECOMP_PATH:
            return ["v31.21"]
        elif path == repo.DECOMP_PATH.joinpath("v31.21"):
            return [
                "convertenomesdecomp_31.21",
                "decomp_31.21",
                "decomp.lic",
                "mpi_decomp31.21.job",
            ]

    mocker.patch("os.listdir", side_effect=mock_listdir)
    r = await repo.list_programs()
    assert r == [
        Program(
            programId="NW0",
            name="NEWAVE",
            clusterId="0",
            version="v29",
            installationDirectory="/home/pem/versoes/NEWAVE/v29",
            isManaged=True,
            executablePath="/home/pem/versoes/NEWAVE/v29/mpi_newave29.job",
            args=["N_PROC"],
        ),
        Program(
            programId="DC0",
            name="DECOMP",
            clusterId="0",
            version="v31.21",
            installationDirectory="/home/pem/versoes/DECOMP/v31.21",
            isManaged=True,
            executablePath="/home/pem/versoes/DECOMP/v31.21/mpi_decomp31.21.job",
            args=["N_PROC"],
        ),
    ]


@pytest.mark.asyncio
async def test_tuber_list_programs(mocker):
    repo = factory("TUBER")
    mock = MagicMock(return_value=True)
    mocker.patch("os.path.isdir", side_effect=mock)

    def mock_listdir(path):
        if path == repo.NEWAVE_PATH:
            return ["v29"]
        elif path == repo.NEWAVE_PATH.joinpath("v29"):
            return [
                "converteexph29",
                "ConverteNomesArquivos",
                "gerenciamento_PLs29",
                "newave29_L",
                "newdesp29_L",
                "nwlistcf29_L",
                "nwlistop29_L",
                "mpi_newave29.job",
            ]
        elif path == repo.DECOMP_PATH:
            return ["v31.21"]
        elif path == repo.DECOMP_PATH.joinpath("v31.21"):
            return [
                "convertenomesdecomp_31.21",
                "decomp_31.21",
                "decomp.lic",
                "mpi_decomp31.21.job",
            ]

    mocker.patch("os.listdir", side_effect=mock_listdir)
    r = await repo.list_programs()
    assert r == [
        Program(
            programId="NW0",
            name="NEWAVE",
            clusterId="0",
            version="v29",
            installationDirectory="/home/pem/versoes/NEWAVE/v29",
            isManaged=True,
            executablePath=repo.NEWAVE_TUBER_JOB + " " + "29",
            args=["N_PROC"],
        ),
        Program(
            programId="DC0",
            name="DECOMP",
            clusterId="0",
            version="v31.21",
            installationDirectory="/home/pem/versoes/DECOMP/v31.21",
            isManaged=True,
            executablePath=repo.DECOMP_TUBER_JOB + " " + "31.21",
            args=["N_PROC"],
        ),
    ]
