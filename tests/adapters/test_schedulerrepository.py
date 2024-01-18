from app.adapters.schedulerrepository import factory
from app.models.job import Job
from app.models.jobstatus import JobStatus
from tests.mocks.scheduler.torque import (
    MockTORQUEListJobs,
    MockTORQUEGetJobRunning,
    MockTORQUEGetJobDone,
    MockTORQUEDeleteJob,
    MockTORQUESubmitJob,
)
from tests.mocks.scheduler.sge import (
    MockSGEListJobs,
    MockSGEGetJobRunning,
    MockSGEGetJobDone,
    MockSGEDeleteJob,
    MockSGESubmitJob,
)
from unittest.mock import AsyncMock
from datetime import datetime, timedelta
import pytest

KB_TO_GB = 1048576
B_TO_GB = 1073741824


@pytest.mark.asyncio
async def test_sge_list_jobs(mocker):
    repo = factory("SGE")
    mock = AsyncMock(return_value=(0, "".join(MockSGEListJobs)))
    mocker.patch(
        "app.adapters.schedulerrepository.run_terminal_retry", side_effect=mock
    )
    r = await repo.list_jobs()
    mock.assert_called_once()
    assert len(r) == 6
    assert r[0].jobId == "1481"
    assert r[0].status == JobStatus.RUNNING
    assert r[0].name == "NEWAVE-v28.16.4_micropen"
    assert r[0].startTime == datetime(
        year=2024, month=1, day=17, hour=15, minute=51, second=53
    )
    assert r[0].endTime is None
    assert r[0].reservedSlots == 64


@pytest.mark.asyncio
async def test_sge_get_running_job(mocker):
    repo = factory("SGE")
    mock = AsyncMock(return_value=(0, "".join(MockSGEGetJobRunning)))
    mocker.patch(
        "app.adapters.schedulerrepository.run_terminal_retry", side_effect=mock
    )
    r = await repo.get_job("1488")
    mock.assert_called_once()
    assert isinstance(r, Job)
    assert r.jobId == "1488"
    assert r.status == JobStatus.UNKNOWN
    assert r.name == "NEWAVE-v28.16.4_micropen"
    assert r.startTime == datetime(
        year=2024, month=1, day=17, hour=13, minute=15, second=50
    )
    assert r.endTime is None
    assert (
        r.workingDirectory
        == "/home/pem/estudos/CPAMP/Ciclo_2023-2024/Backtest/casos/15x40/2021_11_rv0/newave"
    )
    assert r.reservedSlots == 64
    assert r.scriptFile == "/home/pem/rotinas/tuber/jobs/mpi_newave.job"
    cpu = 96002.240000 + 526254.206979
    mem = 322341.641973 + 1678224.580191
    vMem = 5354119168.000000
    maxMem = 42092040192.000000
    memCpu = mem
    instantMem = vMem / B_TO_GB
    maxMem = maxMem / B_TO_GB
    assert r.resourceUsage.cpuSeconds == cpu
    assert r.resourceUsage.memoryCpuSeconds == memCpu
    assert r.resourceUsage.instantTotalMemory == instantMem
    assert r.resourceUsage.maxTotalMemory == maxMem


@pytest.mark.asyncio
async def test_sge_get_finished_job(mocker):
    repo = factory("SGE")
    mock = AsyncMock(return_value=(0, "".join(MockSGEGetJobDone)))
    mocker.patch(
        "app.adapters.schedulerrepository.run_terminal_retry", side_effect=mock
    )
    r = await repo.get_finished_job("1488")
    mock.assert_called_once()
    assert isinstance(r, Job)
    assert r.jobId == "1488"
    assert r.status == JobStatus.STOPPED
    assert r.startTime == datetime(
        year=2024, month=1, day=17, hour=16, minute=15, second=53
    )
    assert r.endTime == datetime(
        year=2024, month=1, day=17, hour=20, minute=12, second=38
    )
    cpu = 631414.377
    mem = 2482282.2939999998
    maxMem = 72362192
    memCpu = mem
    instantMem = 251.60349938626752
    maxMem = 6505.825876953125
    # TODO conferir essas contas
    assert r.resourceUsage.cpuSeconds == cpu
    assert r.resourceUsage.memoryCpuSeconds == memCpu
    assert r.resourceUsage.instantTotalMemory == instantMem
    assert r.resourceUsage.maxTotalMemory == maxMem


@pytest.mark.asyncio
async def test_sge_submit_job(mocker):
    repo = factory("SGE")
    mock = AsyncMock(return_value=(0, "".join(MockSGESubmitJob)))
    mocker.patch(
        "app.adapters.schedulerrepository.run_terminal_retry", side_effect=mock
    )
    jobId = "1488"
    name = "NEWAVE-v28.16.4_micropen"
    scriptFile = "test.job"
    reservedSlots = 16
    jobWorkingDirectory = "/home"
    r = await repo.submit_job(
        Job(
            name=name,
            reservedSlots=reservedSlots,
            scriptFile=scriptFile,
            workingDirectory=jobWorkingDirectory,
            clusterId=1,
            args=[],
        )
    )
    mock.assert_called_once()
    assert isinstance(r, Job)
    assert r.jobId == jobId
    assert r.name == name
    assert r.scriptFile == scriptFile
    assert r.reservedSlots == reservedSlots
    assert r.workingDirectory == jobWorkingDirectory


@pytest.mark.asyncio
async def test_sge_stop_job(mocker):
    repo = factory("SGE")
    mock = AsyncMock(return_value=(0, MockSGEDeleteJob))
    mocker.patch(
        "app.adapters.schedulerrepository.run_terminal_retry", side_effect=mock
    )
    jobId = "90169"
    r = await repo.stop_job(
        jobId,
    )
    mock.assert_called_once()
    assert isinstance(r, Job)
    assert r.jobId == jobId


@pytest.mark.asyncio
async def test_torque_list_jobs(mocker):
    repo = factory("TORQUE")
    mock = AsyncMock(return_value=(0, "".join(MockTORQUEListJobs)))
    mocker.patch(
        "app.adapters.schedulerrepository.run_terminal_retry", side_effect=mock
    )
    r = await repo.list_jobs()
    mock.assert_called_once()
    assert len(r) == 3
    assert r[0].jobId == "90137"
    assert r[0].status == JobStatus.RUNNING
    assert r[0].name == "newave"
    assert r[0].startTime == datetime(
        year=2023, month=5, day=2, hour=10, minute=29, second=37
    )
    assert r[0].endTime is None
    assert (
        r[0].workingDirectory
        == "/home/USER/gpo2/2023/P05_2023/Outros/Sombra_ACL"
    )
    assert r[0].reservedSlots == 96
    assert (
        r[0].scriptFile == "/home/USER/gpo2/versoes/v28.0.3/newave280003.job"
    )
    cpu = 397342
    mem = 34658964
    maxMem = 102001356
    memCpu = cpu * mem / KB_TO_GB
    instantMem = mem / KB_TO_GB
    maxMem = maxMem / KB_TO_GB
    assert r[0].resourceUsage.cpuSeconds == cpu
    assert r[0].resourceUsage.memoryCpuSeconds == memCpu
    assert r[0].resourceUsage.instantTotalMemory == instantMem
    assert r[0].resourceUsage.maxTotalMemory == maxMem


@pytest.mark.asyncio
async def test_torque_get_running_job(mocker):
    repo = factory("TORQUE")
    mock = AsyncMock(return_value=(0, "".join(MockTORQUEGetJobRunning)))
    mocker.patch(
        "app.adapters.schedulerrepository.run_terminal_retry", side_effect=mock
    )
    r = await repo.get_job("87849")
    mock.assert_called_once()
    assert isinstance(r, Job)
    assert r.jobId == "87849"
    assert r.status == JobStatus.RUNNING
    assert r.name == "newave"
    assert r.startTime == datetime(
        year=2023, month=1, day=11, hour=13, minute=54, second=56
    )
    assert r.endTime is None
    assert r.workingDirectory == "/home/USER/gmc2/estudos/versao_28.11/ree"
    assert r.reservedSlots == 32
    assert (
        r.scriptFile == "/home/USER/gmc2/versoes/v28.11_CPAMP/newave_2811.job"
    )
    cpu = 3023
    mem = 7155252
    maxMem = 72251828
    memCpu = cpu * mem / KB_TO_GB
    instantMem = mem / KB_TO_GB
    maxMem = maxMem / KB_TO_GB
    assert r.resourceUsage.cpuSeconds == cpu
    assert r.resourceUsage.memoryCpuSeconds == memCpu
    assert r.resourceUsage.instantTotalMemory == instantMem
    assert r.resourceUsage.maxTotalMemory == maxMem


@pytest.mark.asyncio
async def test_torque_get_finished_job(mocker):
    repo = factory("TORQUE")
    mock = AsyncMock(return_value=(0, "".join(MockTORQUEGetJobDone)))
    mocker.patch(
        "app.adapters.schedulerrepository.run_terminal_retry", side_effect=mock
    )
    r = await repo.get_finished_job("87849")
    mock.assert_called_once()
    assert isinstance(r, Job)
    assert r.jobId == "87849"
    assert r.status == JobStatus.STOPPED
    assert r.startTime == datetime(
        year=2023, month=1, day=11, hour=13, minute=54, second=56
    )
    assert r.endTime == datetime(
        year=2023, month=1, day=11, hour=14, minute=11, second=13
    )
    cpu = 19955
    mem = 30948328
    maxMem = 72362192
    memCpu = cpu * mem / KB_TO_GB
    instantMem = mem / KB_TO_GB
    maxMem = maxMem / KB_TO_GB
    assert r.resourceUsage.cpuSeconds == cpu
    assert r.resourceUsage.memoryCpuSeconds == memCpu
    assert r.resourceUsage.instantTotalMemory == instantMem
    assert r.resourceUsage.maxTotalMemory == maxMem


@pytest.mark.asyncio
async def test_torque_submit_job(mocker):
    repo = factory("TORQUE")
    mock = AsyncMock(return_value=(0, "".join(MockTORQUESubmitJob)))
    mocker.patch(
        "app.adapters.schedulerrepository.run_terminal_retry", side_effect=mock
    )
    jobId = "90169"
    name = "test"
    scriptFile = "test.job"
    reservedSlots = 16
    jobWorkingDirectory = "/home"
    r = await repo.submit_job(
        Job(
            name=name,
            reservedSlots=reservedSlots,
            scriptFile=scriptFile,
            workingDirectory=jobWorkingDirectory,
            clusterId=1,
            args=[],
        )
    )
    mock.assert_called_once()
    assert isinstance(r, Job)
    assert r.jobId == jobId
    assert r.name == name
    assert r.scriptFile == scriptFile
    assert r.reservedSlots == reservedSlots
    assert r.workingDirectory == jobWorkingDirectory


@pytest.mark.asyncio
async def test_torque_stop_job(mocker):
    repo = factory("TORQUE")
    mock = AsyncMock(return_value=(0, MockTORQUEDeleteJob))
    mocker.patch(
        "app.adapters.schedulerrepository.run_terminal_retry", side_effect=mock
    )
    jobId = "90169"
    r = await repo.stop_job(
        jobId,
    )
    mock.assert_called_once()
    assert isinstance(r, Job)
    assert r.jobId == jobId
