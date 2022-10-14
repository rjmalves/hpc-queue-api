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

    STATUS_MAPPING: Dict[str, JobStatus] = {
        "q": JobStatus.START_REQUESTED,
        "qw": JobStatus.START_REQUESTED,
        "t": JobStatus.STARTING,
        "r": JobStatus.RUNNING,
        "d": JobStatus.STOP_REQUESTED,
        "dr": JobStatus.STOPPING,
    }

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
                jobId = job_xml.find("JB_job_number").text
                name = job_xml.find("JB_name").text
                reservedSlots = job_xml.find("slots").text
                if not all([jobId, name, reservedSlots]):
                    continue
                jobs.append(
                    Job(
                        jobId=str(jobId),
                        name=str(name),
                        status=status,
                        startTime=startTime,
                        lastStatusUpdateTime=datetime.now(),
                        clusterId=Settings.clusterId,
                        reservedSlots=int(reservedSlots),
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
        def __parse_get_job(content: str) -> Job:
            try:
                root = ET.fromstring(content)
            except ET.ParseError:
                return None
            jobinfo = root.find("djob_info")
            if not jobinfo:
                return None
            element = jobinfo.find("element")
            if not element:
                return None
            status = SGESchedulerRepository.STATUS_MAPPING.get(
                "", JobStatus.UNKNOWN
            )
            startTime = datetime.fromtimestamp(
                float(element.find("JB_submission_time").text)
            )
            argsList = element.find("JB_job_args")
            argsContent = (
                [a.find("ST_name").text for a in argsList]
                if len(argsList) > 0
                else []
            )
            taskList = (
                element.find("JB_ja_tasks")
                .find("ulong_sublist")
                .find("JAT_task_list")
            )

            usages = []
            if taskList:
                for taskElement in taskList:
                    usageDict = {}
                    usageElement = taskElement.find("PET_scaled_usage")
                    for elem in usageElement:
                        usageDict[elem.find("UA_name").text] = float(
                            elem.find("UA_value").text
                        )
                    usages.append(usageDict)

            jobId = element.find("JB_job_number").text
            name = element.find("JB_job_name").text
            reservedSlots = (
                element.find("JB_pe_range").find("ranges").find("RN_min").text
            )
            workingDirectory = element.find("JB_cwd").text
            scriptFile = element.find("JB_script_file").text
            if not all(
                [jobId, name, reservedSlots, workingDirectory, scriptFile]
            ):
                return None
            # converts total memory from B to GB
            usage = (
                ResourceUsage(
                    cpuSeconds=sum([u["cpu"] for u in usages]),
                    memoryCpuSeconds=sum([u["mem"] for u in usages]),
                    instantTotalMemory=sum([u["vmem"] for u in usages]) / 1e9,
                    maxTotalMemory=sum([u["maxvmem"] for u in usages]) / 1e9,
                    processIO=sum([u["io"] for u in usages]),
                    processIOWaiting=sum([u["iow"] for u in usages]),
                    timeInstant=datetime.now(),
                )
                if usageDict
                else None
            )
            return Job(
                jobId=str(jobId),
                status=status,
                name=str(name),
                startTime=startTime,
                lastStatusUpdateTime=datetime.now(),
                clusterId=Settings.clusterId,
                workingDirectory=str(workingDirectory),
                reservedSlots=int(reservedSlots),
                scriptFile=str(scriptFile),
                args=argsContent,
                resourceUsage=usage,
            )

        cod, ans = await run_terminal_retry([f"qstat -j {jobId} -xml"])
        if cod != 0:
            return None
        else:
            detailedJob = __parse_get_job(ans)
            if detailedJob is None:
                return None
            allJobs = await SGESchedulerRepository.list_jobs()
            generalJobData = [
                j for j in allJobs if j.jobId == detailedJob.jobId
            ]
            if len(generalJobData) == 1:
                detailedJob.status = generalJobData[0].status
                return detailedJob
            else:
                return None

    @staticmethod
    async def submit_job(job: Job) -> Optional[Job]:
        def __parse_submit_ans(content: str):
            job.jobId = content.split("Your job")[1].split("(")[0].strip()
            job.name = content.split("(")[1].split(")")[0].strip('"')

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
            str(job.reservedSlots),
            job.scriptFile,
            *(job.args),
        ]
        with set_directory(job.workingDirectory):
            cod, ans = await run_terminal_retry(command)
        if cod != 0:
            return None
        else:
            __parse_submit_ans(ans)
            return await SGESchedulerRepository.get_job(job.jobId)

    @staticmethod
    async def stop_job(jobId: str) -> Job:
        # TODO - validate required fields
        command = ["qdel", jobId]
        cod, _ = await run_terminal_retry(command)
        if cod != 0:
            return None
        else:
            return await SGESchedulerRepository.get_job(jobId)


SUPPORTED_SCHEDULERS: Dict[str, AbstractSchedulerRepository] = {
    "SGE": SGESchedulerRepository
}
DEFAULT = SGESchedulerRepository


def factory(kind: str) -> AbstractSchedulerRepository:
    s = SUPPORTED_SCHEDULERS.get(kind)
    if s is None:
        return DEFAULT
    return s
