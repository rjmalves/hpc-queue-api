from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from app.internal.settings import Settings
from app.internal.fs import set_directory
from app.models.job import Job, JobStatus
from app.models.resourceusage import ResourceUsage
from app.internal.terminal import run_terminal_retry
import xml.etree.ElementTree as ET


class AbstractSchedulerRepository(ABC):
    """ """

    @staticmethod
    @abstractmethod
    async def list_jobs() -> List[Job]:
        pass

    @staticmethod
    @abstractmethod
    async def get_job(jobId: str) -> Job:
        pass

    @staticmethod
    @abstractmethod
    async def submit_job(job: Job) -> Job:
        pass

    @staticmethod
    @abstractmethod
    async def stop_job(jobId: str) -> Job:
        pass


class SGESchedulerRepository(AbstractSchedulerRepository):
    """"""

    DUMMY_JOB = Job(
        jobId="1",
        name="test",
        status=JobStatus.RUNNING,
        startTime=datetime.now(),
        lastStatusUpdateTime=datetime.now(),
        clusterId="1",
        workingDirectory="/home/pem/test",
        reservedSlots=96,
        scriptFile="/home/pem/newave",
        resourceUsage=None,
    )

    STATUS_MAPPING: Dict[str, JobStatus] = {}

    # TODO - get example of qstat -t

    @staticmethod
    async def list_jobs() -> Optional[List[Job]]:
        def __parse_list_jobs(content: str) -> List[Job]:
            root = ET.fromstring(content)
            jobs: List[Job] = []
            for job_xml in root[0]:
                status = SGESchedulerRepository.STATUS_MAPPING.get(
                    job_xml.find("state").text, JobStatus.UNKNOWN
                )
                startTime = datetime.fromisoformat(
                    job_xml.find("JAT_start_time").text
                )
                jobs.append(
                    Job(
                        jobId=job_xml.find("JB_job_number").text,
                        name=job_xml.find("JB_name").text,
                        status=status,
                        startTime=startTime,
                        lastStatusUpdateTime=datetime.now(),
                        clusterId=Settings.clusterId,
                        reservedSlots=int(job_xml.find("slots").text),
                    )
                )
            return jobs

        cod, ans = await run_terminal_retry(["qstat -xml"])
        if cod != 0:
            return None
        else:
            return __parse_list_jobs(ans)

    @staticmethod
    async def get_job(jobId: str) -> Optional[Job]:
        def __parse_get_job(content: str) -> List[Job]:
            root = ET.fromstring(content)
            jobinfo = root.find("djob_info")
            if not jobinfo:
                return None
            element = jobinfo.find("element")
            if not element:
                return None
            status = SGESchedulerRepository.STATUS_MAPPING.get(
                element.find("state").text, JobStatus.UNKNOWN
            )
            startTime = datetime.fromtimestamp(
                float(element.find("JB_submission_time").text)
            )
            argsList = element.find("JB_job_args")
            usageList = (
                element.find("JB_ja_tasks")
                .find("ulong_sublist")
                .find("JAT_scaled_usage_list")
            )
            usageDict = {}
            for scaled in usageList:
                usageDict[scaled.find("UA_name").text] = float(
                    scaled.find("UA_value").text
                )
            # TODO - normalize units
            usage = ResourceUsage(
                cpu=usageDict["cpu"],
                memory=usageDict["mem"],
                io=usageDict["io"],
            )
            return Job(
                jobId=element.find("JB_job_number").text,
                status=status,
                name=element.find("JB_job_name").text,
                startTime=startTime,
                lastStatusUpdateTime=datetime.now(),
                clusterId=Settings.clusterId,
                workingDirectory=element.find("JB_cwd").text,
                reservedSlots=int(
                    element.find("JB_pe_range")
                    .find("ranges")
                    .find("RN_min")
                    .text
                ),
                scriptFile=element.find("JB_script_file").text,
                args=[a.text for a in argsList],
                resourceUsage=usage,
            )

        cod, ans = await run_terminal_retry([f"qstat -j {jobId} -xml"])
        if cod != 0:
            return None
        else:
            return __parse_get_job(ans)

    @staticmethod
    async def submit_job(job: Job) -> Optional[Job]:
        def __parse_submit_ans(content: str):
            job.jobId = content.split("Your job")[1].split("(")[0].strip()
            job.name = content.split("(")[1].split(")")[0].strip('"')

        # TODO - validate required fields
        if not job.name:
            job.name = Path(job.workingDirectory).parts[-1]
        command = [
            "qsub",
            "-cwd",
            "-V",
            "-N",
            job.name,
            "-pe",
            "orte",
            job.reservedSlots,
            job.scriptFile,
            *(job.args),
        ]
        with set_directory(job.workingDirectory):
            cod, ans = await run_terminal_retry(command)
        if cod != 0:
            return None
        else:
            __parse_submit_ans(ans)
            return SGESchedulerRepository.get_job(job.jobId)

    @staticmethod
    async def stop_job(jobId: str) -> Job:
        # TODO - validate required fields
        command = ["qdel", jobId]
        cod, _ = await run_terminal_retry(command)
        if cod != 0:
            return None
        else:
            return SGESchedulerRepository.get_job(jobId)


SUPPORTED_SCHEDULERS: Dict[str, AbstractSchedulerRepository] = {
    "SGE": SGESchedulerRepository
}
DEFAULT = SGESchedulerRepository


def factory(kind: str) -> AbstractSchedulerRepository:
    s = SUPPORTED_SCHEDULERS.get(kind)
    if s is None:
        return DEFAULT
    return s
