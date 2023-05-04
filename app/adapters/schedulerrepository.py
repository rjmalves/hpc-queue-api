from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Union

from app.internal.settings import Settings
from app.internal.fs import set_directory
from app.internal.httpresponse import HTTPResponse
from app.models.job import Job, JobStatus
from app.models.resourceusage import ResourceUsage
from app.internal.terminal import run_terminal_retry
import xml.etree.ElementTree as ET


class AbstractSchedulerRepository(ABC):
    """ """

    @staticmethod
    @abstractmethod
    async def list_jobs() -> Union[List[Job], HTTPResponse]:
        pass

    @staticmethod
    @abstractmethod
    async def get_job(jobId: str) -> Union[Job, HTTPResponse]:
        pass

    @staticmethod
    @abstractmethod
    async def get_finished_job(jobId: str) -> Union[Job, HTTPResponse]:
        pass

    @staticmethod
    @abstractmethod
    async def submit_job(job: Job) -> Union[Job, HTTPResponse]:
        pass

    @staticmethod
    @abstractmethod
    async def stop_job(jobId: str) -> Union[Job, HTTPResponse]:
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
    async def list_jobs() -> Union[List[Job], HTTPResponse]:
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
            return HTTPResponse(code=500, detail=f"error running qstat: {ans}")
        else:
            return __parse_list_jobs(ans)

    @staticmethod
    async def get_job(jobId: str) -> Union[Job, HTTPResponse]:
        def __parse_get_job(content: str) -> Union[Job, HTTPResponse]:
            try:
                root = ET.fromstring(content)
            except Exception:
                return HTTPResponse(
                    code=500, detail="error parsing qstat response"
                )
            jobinfo = root.find("djob_info")
            if not jobinfo:
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            element = jobinfo.find("element")
            if not element:
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            status = SGESchedulerRepository.STATUS_MAPPING.get(
                "", JobStatus.UNKNOWN
            )
            startTime = datetime.fromtimestamp(
                float(element.find("JB_submission_time").text)
            )
            argsList = element.find("JB_job_args")
            argsContent = (
                [a.find("ST_name").text for a in argsList] if argsList else []
            )
            taskList = None
            masterUsageList = None
            jobTasks = element.find("JB_ja_tasks")
            if jobTasks:
                taskSublist = jobTasks.find("ulong_sublist")
                if taskSublist:
                    masterUsageList = taskSublist.find("JAT_scaled_usage_list")
                    taskList = taskSublist.find("JAT_task_list")

            usages = []
            if masterUsageList:
                usageDict = {}
                for scaled in masterUsageList:
                    usageDict[scaled.find("UA_name").text] = float(
                        scaled.find("UA_value").text
                    )
                usages.append(usageDict)
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
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            # converts total memory from B to GB
            B_TO_GB = 1073741824
            usage = (
                ResourceUsage(
                    cpuSeconds=sum([u.get("cpu", 0.0) for u in usages]),
                    memoryCpuSeconds=sum([u.get("mem", 0.0) for u in usages]),
                    instantTotalMemory=sum(
                        [u.get("vmem", 0.0) for u in usages]
                    )
                    / B_TO_GB,
                    maxTotalMemory=sum([u.get("maxvmem", 0.0) for u in usages])
                    / B_TO_GB,
                    processIO=sum([u.get("io", 0.0) for u in usages]),
                    processIOWaiting=sum([u.get("iow", 0.0) for u in usages]),
                    timeInstant=datetime.now(),
                )
                if len(usages) > 0
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
            return HTTPResponse(
                code=500, detail=f"error running qstat command: {ans}"
            )
        else:
            detailedJob = __parse_get_job(ans)
            if isinstance(detailedJob, HTTPResponse):
                return HTTPResponse(
                    code=500, detail="error parsing qstat -j result"
                )
            else:
                return detailedJob

    @staticmethod
    async def get_finished_job(jobId: str) -> Union[Job, HTTPResponse]:
        def __parse_get_job(content: str) -> Union[Job, HTTPResponse]:
            # Iterates for getting info in the nodes
            nameStr = "jobname  "
            startTimeStr = "start_time  "
            endTimeStr = "end_time    "
            slotsStr = "slots   "
            cpuStr = "cpu      "
            memStr = "mem      "
            ioStr = "io      "
            iowStr = "iow     "
            maxVmemStr = "maxvmem   "
            name = None
            startTime = None
            endTime = None
            slots = None
            cpu = 0.0
            mem = 0.0
            io = 0.0
            iow = 0.0
            maxvmem = 0.0
            unitMultipliers = {
                "k": 1048576.0,
                "M": 1024.0,
                "G": 1.0,
            }
            for line in content.split("\n"):
                if nameStr in line:
                    name = line[13:].strip()
                elif startTimeStr in line:
                    startTime = datetime.strptime(
                        line[13:].strip(), "%a %b %d %H:%M:%S %Y"
                    )
                elif endTimeStr in line:
                    endTime = datetime.strptime(
                        line[13:].strip(), "%a %b %d %H:%M:%S %Y"
                    )
                elif slotsStr in line:
                    slots = int(line[13:].strip())
                elif cpuStr in line:
                    cpu += float(line[13:].strip())
                elif memStr in line and maxVmemStr not in line:
                    mem += float(line[13:].strip())
                elif ioStr in line:
                    io += float(line[13:].strip())
                elif iowStr in line:
                    iow += float(line[13:].strip())
                elif maxVmemStr in line:
                    unit = line[13:].strip()[-1]
                    maxvmem += float(
                        line[13:].strip()[:-1]
                    ) / unitMultipliers.get(unit, 1073741824.0)

            if not all([name, startTime, endTime, slots]):
                return HTTPResponse(
                    code=500,
                    detail=f"error parsing qacct -j response: {content}",
                )

            memUsage = float(mem / cpu * slots if cpu > 0.0 else 0.0)
            usage = ResourceUsage(
                cpuSeconds=cpu,
                memoryCpuSeconds=mem,
                instantTotalMemory=memUsage,
                maxTotalMemory=maxvmem,
                processIO=io,
                processIOWaiting=iow,
                timeInstant=endTime,
            )

            return Job(
                jobId=jobId,
                status=JobStatus.STOPPED,
                name=str(name),
                startTime=startTime,
                lastStatusUpdateTime=endTime,
                endTime=endTime,
                clusterId=Settings.clusterId,
                reservedSlots=int(slots),
                resourceUsage=usage,
            )

        cod, ans = await run_terminal_retry([f"qacct -j {jobId}"])
        if cod != 0:
            return HTTPResponse(code=404, detail=f"job {jobId} not found")
        else:
            detailedJob = __parse_get_job(ans)
            return detailedJob

    @staticmethod
    async def submit_job(job: Job) -> Union[Job, HTTPResponse]:
        def __parse_submit_ans(content: str):
            job.jobId = content.split("Your job")[1].split("(")[0].strip()
            job.name = content.split("(")[1].split(")")[0].strip('"')

        if not job.name:
            job.name = Path(job.workingDirectory).parts[-1]
        if not job.workingDirectory:
            return HTTPResponse(
                code=400, detail="workingDirectory is mandatory"
            )
        if not job.reservedSlots:
            return HTTPResponse(code=400, detail="reservedSlots is mandatory")
        if not job.scriptFile:
            return HTTPResponse(code=400, detail="scriptFile is mandatory")
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
            return HTTPResponse(
                code=500, detail=f"error running qsub command: {ans}"
            )
        else:
            __parse_submit_ans(ans)
            return job

    @staticmethod
    async def stop_job(jobId: str) -> Union[Job, HTTPResponse]:
        # TODO - validate required fields
        command = ["qdel", jobId]
        cod, ans = await run_terminal_retry(command)
        if cod != 0:
            return HTTPResponse(
                code=500, detail=f"error running qdel command: {ans}"
            )
        else:
            return Job(jobId=jobId, clusterId=Settings.clusterId)


class TorqueSchedulerRepository(AbstractSchedulerRepository):
    """"""

    STATUS_MAPPING: Dict[str, JobStatus] = {
        "Q": JobStatus.START_REQUESTED,
        "W": JobStatus.START_REQUESTED,
        "T": JobStatus.STARTING,
        "R": JobStatus.RUNNING,
        "H": JobStatus.STOP_REQUESTED,
        "E": JobStatus.STOPPING,
        "C": JobStatus.STOPPED,
    }

    KB_TO_GB = 1048576

    @staticmethod
    def __parse_to_timedelta(time_str: str) -> timedelta:
        hour, minute, second = time_str.split(":")
        return timedelta(
            hours=int(hour), minutes=int(minute), seconds=int(second)
        )

    @staticmethod
    def __parse_to_datetime(time_str: str) -> datetime:
        return datetime.strptime(time_str, "%a %b %d %H:%M:%S %Y")

    @staticmethod
    async def list_jobs() -> Union[List[Job], HTTPResponse]:
        def __parse_list_jobs(content: str) -> List[Job]:
            NEW_JOB_PATTERN = "Job Id:"
            JOB_NAME_PATTERN = "Job_Name ="
            JOB_STATUS_PATTERN = "job_state ="
            JOB_START_TIME_PATTERN = "start_time ="
            JOB_SLOTS_PATTERN = "Resource_List.nodes ="
            JOB_WORKING_DIR_PATTERN = "Output_Path ="
            JOB_ARGS_PATTERN = "submit_args ="
            JOB_RESOURCE_PATTERN = "resources_used."
            lines = content.split("\n")
            jobs: List[Job] = []
            if len(lines) < 3:
                return jobs
            jobId = None
            name = None
            status = None
            startTime = None
            reservedSlots = None
            workingDirectory = None
            scriptFile = None
            jobArgs = None
            resources = {
                "cput": None,
                "mem": None,
                "vmem": None,
            }
            for line in lines:
                if len(line) == 0:
                    if jobId is not None:
                        jobs.append(Job)
                if len(line) < 5:
                    continue
                if NEW_JOB_PATTERN in line:
                    jobId = line[7:].strip().split(".")[0]
                elif JOB_NAME_PATTERN in line:
                    name = line.split(JOB_NAME_PATTERN)[1].strip()
                elif JOB_STATUS_PATTERN in line:
                    status = TorqueSchedulerRepository.STATUS_MAPPING.get(
                        line.split(JOB_STATUS_PATTERN)[1].strip(),
                        JobStatus.UNKNOWN,
                    )
                elif JOB_START_TIME_PATTERN in line:
                    startTime = TorqueSchedulerRepository.__parse_to_datetime(
                        line.split(JOB_START_TIME_PATTERN)[1].strip()
                    )
                elif JOB_SLOTS_PATTERN in line:
                    slotData = (
                        line.split(JOB_SLOTS_PATTERN)[1].strip().split(":ppn=")
                    )
                    reservedSlots = int(slotData[0]) * int(slotData[1])
                elif JOB_WORKING_DIR_PATTERN in line:
                    outputPath = Path(
                        line.split(JOB_WORKING_DIR_PATTERN)[1]
                        .strip()
                        .split(":")[1]
                    )
                    workingDirectory = str(outputPath.parent)
                elif JOB_ARGS_PATTERN in line:
                    args = line.split(JOB_ARGS_PATTERN)[1].strip().split(" ")
                    scriptFile = args[0]
                    jobArgs = args[1:]
                elif JOB_RESOURCE_PATTERN in line:
                    args = line.split(JOB_ARGS_PATTERN)[1].split("=")
                    if args[0].strip() == "cput":
                        cpuTime = (
                            TorqueSchedulerRepository.__parse_to_timedelta(
                                args[1].strip()
                            )
                        )
                        resources["cput"] = cpuTime.total_seconds()
                    elif args[0].strip() == "mem":
                        mem = (
                            int(args[1].strip().split("kb")[0])
                            / TorqueSchedulerRepository.KB_TO_GB
                        )

                        resources["mem"] = mem
                    elif args[0].strip() == "vmem":
                        mem = (
                            int(args[1].strip().split("kb")[0])
                            / TorqueSchedulerRepository.KB_TO_GB
                        )
                        resources["vmem"] = mem

                jobs.append(
                    Job(
                        jobId=str(jobId),
                        name=str(name),
                        status=status,
                        startTime=startTime,
                        lastStatusUpdateTime=datetime.now(),
                        clusterId=Settings.clusterId,
                        workingDirectory=workingDirectory,
                        reservedSlots=int(reservedSlots),
                        scriptFile=scriptFile,
                        jobArgs=jobArgs,
                        resourceUsage=ResourceUsage(
                            cpuSeconds=resources["cput"],
                            memoryCpuSeconds=resources["cput"]
                            * resources["mem"],
                            instantTotalMemory=resources["mem"],
                            maxTotalMemory=resources["vmem"],
                            processIO=0.0,
                            processIOWaiting=0.0,
                            timeInstant=datetime.now(),
                        ),
                    )
                )
            return jobs

        cod, ans = await run_terminal_retry(["qstat -f"])
        if cod != 0:
            return HTTPResponse(code=500, detail=f"error running qstat: {ans}")
        else:
            return __parse_list_jobs(ans)

    @staticmethod
    async def get_job(jobId: str) -> Union[Job, HTTPResponse]:
        def __parse_get_job(content: str) -> Job:
            NEW_JOB_PATTERN = "Job Id:"
            JOB_NAME_PATTERN = "Job_Name ="
            JOB_STATUS_PATTERN = "job_state ="
            JOB_START_TIME_PATTERN = "start_time ="
            JOB_SLOTS_PATTERN = "Resource_List.nodes ="
            JOB_WORKING_DIR_PATTERN = "Output_Path ="
            JOB_ARGS_PATTERN = "submit_args ="
            JOB_RESOURCE_PATTERN = "resources_used."
            lines = content.split("\n")
            jobs: List[Job] = []
            if len(lines) < 3:
                return jobs
            jobId = None
            name = None
            status = None
            startTime = None
            reservedSlots = None
            workingDirectory = None
            scriptFile = None
            jobArgs = None
            resources = {
                "cput": None,
                "mem": None,
                "vmem": None,
            }
            for line in lines:
                if len(line) == 0:
                    if jobId is not None:
                        jobs.append(Job)
                if len(line) < 5:
                    continue
                if NEW_JOB_PATTERN in line:
                    jobId = line[7:].strip().split(".")[0]
                elif JOB_NAME_PATTERN in line:
                    name = line.split(JOB_NAME_PATTERN)[1].strip()
                elif JOB_STATUS_PATTERN in line:
                    status = TorqueSchedulerRepository.STATUS_MAPPING.get(
                        line.split(JOB_STATUS_PATTERN)[1].strip(),
                        JobStatus.UNKNOWN,
                    )
                elif JOB_START_TIME_PATTERN in line:
                    startTime = TorqueSchedulerRepository.__parse_to_datetime(
                        line.split(JOB_START_TIME_PATTERN)[1].strip()
                    )
                elif JOB_SLOTS_PATTERN in line:
                    slotData = (
                        line.split(JOB_SLOTS_PATTERN)[1].strip().split(":ppn=")
                    )
                    reservedSlots = int(slotData[0]) * int(slotData[1])
                elif JOB_WORKING_DIR_PATTERN in line:
                    outputPath = Path(
                        line.split(JOB_WORKING_DIR_PATTERN)[1]
                        .strip()
                        .split(":")[1]
                    )
                    workingDirectory = str(outputPath.parent)
                elif JOB_ARGS_PATTERN in line:
                    args = line.split(JOB_ARGS_PATTERN)[1].strip().split(" ")
                    scriptFile = args[0]
                    jobArgs = args[1:]
                elif JOB_RESOURCE_PATTERN in line:
                    args = line.split(JOB_ARGS_PATTERN)[1].split("=")
                    if args[0].strip() == "cput":
                        cpuTime = (
                            TorqueSchedulerRepository.__parse_to_timedelta(
                                args[1].strip()
                            )
                        )
                        resources["cput"] = cpuTime.total_seconds()
                    elif args[0].strip() == "mem":
                        mem = (
                            int(args[1].strip().split("kb")[0])
                            / TorqueSchedulerRepository.KB_TO_GB
                        )

                        resources["mem"] = mem
                    elif args[0].strip() == "vmem":
                        mem = (
                            int(args[1].strip().split("kb")[0])
                            / TorqueSchedulerRepository.KB_TO_GB
                        )
                        resources["vmem"] = mem

                jobs.append(
                    Job(
                        jobId=str(jobId),
                        name=str(name),
                        status=status,
                        startTime=startTime,
                        lastStatusUpdateTime=datetime.now(),
                        clusterId=Settings.clusterId,
                        workingDirectory=workingDirectory,
                        reservedSlots=int(reservedSlots),
                        scriptFile=scriptFile,
                        jobArgs=jobArgs,
                        resourceUsage=ResourceUsage(
                            cpuSeconds=resources["cput"],
                            memoryCpuSeconds=resources["cput"]
                            * resources["mem"],
                            instantTotalMemory=resources["mem"],
                            maxTotalMemory=resources["vmem"],
                            processIO=0.0,
                            processIOWaiting=0.0,
                            timeInstant=datetime.now(),
                        ),
                    )
                )
            if len(jobs) != 1:
                return HTTPResponse(500, "")
            else:
                return jobs[0]

        cod, ans = await run_terminal_retry([f"qstat -f {jobId}"])
        if cod != 0:
            return HTTPResponse(
                code=500, detail=f"error running qstat command: {ans}"
            )
        else:
            detailedJob = __parse_get_job(ans)
            if isinstance(detailedJob, HTTPResponse):
                return HTTPResponse(
                    code=500, detail="error parsing qstat -j result"
                )
            else:
                return detailedJob

    @staticmethod
    async def get_finished_job(jobId: str) -> Union[Job, HTTPResponse]:
        def __parse_get_job(content: str) -> Union[Job, HTTPResponse]:
            # Iterates for getting info in the nodes
            startTimeStr = "Job Run"
            resourcesStr = "Exit_status="
            timeStr = "resources_used.walltime="
            cpuStr = "resources_used.cput="
            memStr = "resources_used.mem="
            maxVmemStr = "resources_used.vmem="
            name = ""
            startTime = None
            endTime = None
            slots = 0
            cpu = 0.0
            mem = 0.0
            io = 0.0
            iow = 0.0
            maxvmem = 0.0
            unitMultipliers = {
                "kb": 1048576.0,
                "M": 1024.0,
                "G": 1.0,
            }
            for line in content.split("\n"):
                print(line)
                if startTimeStr in line:
                    startTime = datetime.strptime(
                        line[:19].strip(), "%m/%d/%Y %H:%M:%S"
                    )
                elif resourcesStr in line:
                    # Filtra os trechos de cada informação
                    cpu = float(line.split(cpuStr)[1].split(" ")[0])
                    memdata = line.split(memStr)[1].split(" ")[0]
                    mem = float(memdata[:-2]) / unitMultipliers[memdata[-2:]]
                    vmemdata = line.split(maxVmemStr)[1].split(" ")[0]
                    maxvmem = (
                        float(vmemdata[:-2]) / unitMultipliers[vmemdata[-2:]]
                    )
                    runtime = TorqueSchedulerRepository.__parse_to_datetime(
                        line.split(timeStr)[1].split(" ")[0]
                    )
                    endTime = startTime + runtime
            if not all([startTime, endTime]):
                return HTTPResponse(
                    code=500,
                    detail=f"error parsing tracejob response: {content}",
                )

            usage = ResourceUsage(
                cpuSeconds=cpu,
                memoryCpuSeconds=mem * cpu,
                instantTotalMemory=mem,
                maxTotalMemory=maxvmem,
                processIO=io,
                processIOWaiting=iow,
                timeInstant=endTime,
            )

            return Job(
                jobId=jobId,
                status=JobStatus.STOPPED,
                name=str(name),
                startTime=startTime,
                lastStatusUpdateTime=endTime,
                endTime=endTime,
                clusterId=Settings.clusterId,
                reservedSlots=int(slots),
                resourceUsage=usage,
            )

        cod, ans = await run_terminal_retry([f"tracejob {jobId}"])
        if cod != 0:
            return HTTPResponse(code=404, detail=f"job {jobId} not found")
        else:
            detailedJob = __parse_get_job(ans)
            return detailedJob

    @staticmethod
    async def submit_job(job: Job) -> Union[Job, HTTPResponse]:
        def __parse_submit_ans(content: str):
            job.jobId = content.split(".")[0].strip()

        if not job.name:
            job.name = Path(job.workingDirectory).parts[-1]
        if not job.workingDirectory:
            return HTTPResponse(
                code=400, detail="workingDirectory is mandatory"
            )
        if not job.reservedSlots:
            return HTTPResponse(code=400, detail="reservedSlots is mandatory")
        if not job.scriptFile:
            return HTTPResponse(code=400, detail="scriptFile is mandatory")
        command = [
            "qsub",
            job.scriptFile,
            "-N",
            job.name,
            "-l",
            f"nodes={job.reservedSlots}",
            *(job.args),
        ]
        with set_directory(job.workingDirectory):
            cod, ans = await run_terminal_retry(command)
        if cod != 0:
            return HTTPResponse(
                code=500, detail=f"error running qsub command: {ans}"
            )
        else:
            __parse_submit_ans(ans)
            return job

    @staticmethod
    async def stop_job(jobId: str) -> Union[Job, HTTPResponse]:
        # TODO - validate required fields
        command = ["qdel", jobId]
        cod, ans = await run_terminal_retry(command)
        if cod != 0:
            return HTTPResponse(
                code=500, detail=f"error running qdel command: {ans}"
            )
        else:
            return Job(jobId=jobId, clusterId=Settings.clusterId)


SUPPORTED_SCHEDULERS: Dict[str, AbstractSchedulerRepository] = {
    "SGE": SGESchedulerRepository,
    "TORQUE": TorqueSchedulerRepository,
}
DEFAULT = SGESchedulerRepository


def factory(kind: str) -> AbstractSchedulerRepository:
    s = SUPPORTED_SCHEDULERS.get(kind)
    if s is None:
        return DEFAULT
    return s
