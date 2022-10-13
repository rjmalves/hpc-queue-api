from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional
from app.internal.settings import Settings

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
    async def get_job(jobId: str) -> Job:
        # TODO - parse job details
        return SGESchedulerRepository.DUMMY_JOB

    @staticmethod
    async def submit_job(job: Job) -> Job:
        # TODO - format qsub command
        job.jobId = 1
        job.status = JobStatus.START_REQUESTED
        job.lastStatusUpdateTime = datetime.now()
        job.resourceUsage = ResourceUsage(
            cpu=5.0, memory=10.0, io=0.0, timeInstant=datetime.now()
        )
        return job

    @staticmethod
    async def stop_job(jobId: str) -> Job:
        # TODO - format qdel command
        return SGESchedulerRepository.DUMMY_JOB


SUPPORTED_SCHEDULERS: Dict[str, AbstractSchedulerRepository] = {
    "SGE": SGESchedulerRepository
}
DEFAULT = SGESchedulerRepository


def factory(kind: str) -> AbstractSchedulerRepository:
    s = SUPPORTED_SCHEDULERS.get(kind)
    if s is None:
        return DEFAULT
    return s
