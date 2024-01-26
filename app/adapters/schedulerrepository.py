from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Union, Type
from os.path import isdir
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
                state = job_xml.find("state")
                jatStart = job_xml.find("JAT_start_time")
                jbNumber = job_xml.find("JB_job_number")
                jbName = job_xml.find("JB_name")
                slots = job_xml.find("slots")
                if (
                    state is None
                    or jatStart is None
                    or jbNumber is None
                    or jbName is None
                    or slots is None
                ):
                    continue
                if state.text is None or jatStart.text is None:
                    continue
                status = SGESchedulerRepository.STATUS_MAPPING.get(
                    state.text, JobStatus.UNKNOWN
                )
                startTime = datetime.fromisoformat(jatStart.text)
                jobId = jbNumber.text
                name = jbName.text
                reservedSlots = slots.text
                if jobId is None or name is None or reservedSlots is None:
                    continue
                jobs.append(
                    Job(
                        jobId=str(jobId),
                        name=str(name),
                        status=status,
                        startTime=startTime,
                        lastStatusUpdateTime=datetime.now(),
                        endTime=None,
                        clusterId=Settings.clusterId,
                        workingDirectory=None,
                        scriptFile=None,
                        reservedSlots=int(reservedSlots),
                        resourceUsage=None,
                        args=None,
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
            subTime = element.find("JB_submission_time")
            if subTime is None:
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            if subTime.text is None:
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            startTime = datetime.fromtimestamp(float(subTime.text))
            argsList = element.find("JB_job_args")
            if argsList is None:
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            argsContent = []
            for a in argsList:
                stName = a.find("ST_name")
                if stName is not None:
                    stText = stName.text
                    if stText is not None:
                        argsContent.append(stText)

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
                    uaName = scaled.find("UA_name")
                    uaValue = scaled.find("UA_value")
                    if uaName is None or uaValue is None:
                        continue
                    if uaName.text is None or uaValue.text is None:
                        continue
                    usageDict[uaName.text] = float(uaValue.text)
                usages.append(usageDict)
            if taskList:
                for taskElement in taskList:
                    usageDict = {}
                    usageElement = taskElement.find("PET_scaled_usage")
                    if usageElement is None:
                        continue
                    for elem in usageElement:
                        if elem is None:
                            continue
                        uaName = elem.find("UA_name")
                        uaValue = elem.find("UA_value")
                        if uaName is None or uaValue is None:
                            continue
                        if uaName.text is None or uaValue.text is None:
                            continue
                        usageDict[uaName.text] = float(uaValue.text)
                    usages.append(usageDict)
            jbNumber = element.find("JB_job_number")
            jbName = element.find("JB_job_name")
            if jbNumber is None:
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            if jbName is None:
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            jobId = jbNumber.text
            name = jbName.text
            jbRange = element.find("JB_pe_range")
            if jbRange is None:
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            ranges = jbRange.find("ranges")
            if ranges is None:
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            rnmin = ranges.find("RN_min")
            if rnmin is None:
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            reservedSlots = rnmin.text
            cwd = element.find("JB_cwd")
            sfile = element.find("JB_script_file")
            if cwd is None or sfile is None:
                return HTTPResponse(
                    code=503, detail="detailed job info not yet available"
                )
            workingDirectory = cwd.text
            scriptFile = sfile.text
            if (
                jobId is None
                or name is None
                or reservedSlots is None
                or workingDirectory is None
                or scriptFile is None
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
                endTime=None,
                clusterId=Settings.clusterId,
                workingDirectory=str(workingDirectory),
                reservedSlots=int(reservedSlots),
                scriptFile=str(scriptFile),
                args=[a for a in argsContent if a is not None],
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

            if (
                name is None
                or startTime is None
                or endTime is None
                or slots is None
            ):
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
                workingDirectory=None,
                reservedSlots=int(slots),
                scriptFile=None,
                resourceUsage=usage,
                args=None,
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

        if not job.workingDirectory:
            return HTTPResponse(
                code=400, detail="workingDirectory is mandatory"
            )
        if not job.name:
            job.name = Path(job.workingDirectory).parts[-1]
        if not job.reservedSlots:
            return HTTPResponse(code=400, detail="reservedSlots is mandatory")
        if not job.scriptFile:
            return HTTPResponse(code=400, detail="scriptFile is mandatory")
        args = job.args if job.args is not None else []
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
            *args,
        ]
        if not isdir(job.workingDirectory):
            return HTTPResponse(
                code=400,
                detail=f"directory {job.workingDirectory} does not exist",
            )
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
            return Job(
                jobId=jobId,
                status=None,
                name=None,
                startTime=None,
                lastStatusUpdateTime=None,
                endTime=None,
                clusterId=Settings.clusterId,
                workingDirectory=None,
                reservedSlots=None,
                scriptFile=None,
                args=None,
                resourceUsage=None,
            )


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
    MAX_LINE_LENGTH = 78

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
        def __parse_list_jobs(content: str) -> Union[List[Job], HTTPResponse]:
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
                "cput": 0.0,
                "mem": 0.0,
                "vmem": 0.0,
            }
            for idx, line in enumerate(lines):
                if len(line) == 0:
                    if jobId is not None and not any(
                        [j.jobId == jobId for j in jobs]
                    ):
                        jobs.append(
                            Job(
                                jobId=str(jobId),
                                name=str(name),
                                status=status,
                                startTime=startTime,
                                lastStatusUpdateTime=datetime.now(),
                                endTime=None,
                                clusterId=Settings.clusterId,
                                workingDirectory=workingDirectory,
                                reservedSlots=int(reservedSlots),
                                scriptFile=scriptFile,
                                args=jobArgs,
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
                    # Se atinge o tamanho máximo, pode continuar na linha
                    # seguinte. Continua até achar o padrão do próximo dado.
                    num_linhas = 1
                    continuacao = ""
                    prox_linha = lines[idx + num_linhas].strip()
                    while "Priority =" not in prox_linha:
                        continuacao += prox_linha
                        num_linhas += 1
                        prox_linha = lines[idx + num_linhas].strip()
                    outputPath = Path(
                        (
                            line.split(JOB_WORKING_DIR_PATTERN)[1].strip()
                            + continuacao
                        ).split(":")[1]
                    )
                    workingDirectory = str(outputPath.parent)
                elif JOB_ARGS_PATTERN in line:
                    # Se atinge o tamanho máximo, pode continuar na linha
                    # seguinte. Continua até achar o padrão do próximo dado.
                    num_linhas = 1
                    continuacao = ""
                    prox_linha = lines[idx + num_linhas].strip()
                    while "start_time =" not in prox_linha:
                        continuacao += prox_linha
                        num_linhas += 1
                        prox_linha = lines[idx + num_linhas].strip()
                    args = (
                        line.split(JOB_ARGS_PATTERN)[1].strip() + continuacao
                    ).split(" ")
                    scriptFile = args[0]
                    jobArgs = args[1:]
                elif JOB_RESOURCE_PATTERN in line:
                    args = line.split(JOB_RESOURCE_PATTERN)[1].split("=")
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

            return jobs

        cod, ans = await run_terminal_retry(["qstat -f"])
        if cod != 0:
            return HTTPResponse(code=500, detail=f"error running qstat: {ans}")
        else:
            return __parse_list_jobs(ans)

    @staticmethod
    async def get_job(jobId: str) -> Union[Job, HTTPResponse]:
        def __parse_get_job(content: str) -> Union[Job, HTTPResponse]:
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
                return HTTPResponse(code=404, detail="no jobs found")
            jobId = None
            name = None
            status = None
            startTime = None
            reservedSlots = None
            workingDirectory = None
            scriptFile = None
            jobArgs = None
            resources = {
                "cput": 0.0,
                "mem": 0.0,
                "vmem": 0.0,
            }
            for idx, line in enumerate(lines):
                if len(line) == 0:
                    if jobId is not None and not any(
                        [j.jobId == jobId for j in jobs]
                    ):
                        jobs.append(
                            Job(
                                jobId=str(jobId),
                                name=str(name),
                                status=status,
                                startTime=startTime,
                                lastStatusUpdateTime=datetime.now(),
                                endTime=None,
                                clusterId=Settings.clusterId,
                                workingDirectory=workingDirectory,
                                reservedSlots=int(reservedSlots),
                                scriptFile=scriptFile,
                                args=jobArgs,
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
                        break
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
                    # Se atinge o tamanho máximo, pode continuar na linha
                    # seguinte. Continua até achar o padrão do próximo dado.
                    num_linhas = 1
                    continuacao = ""
                    prox_linha = lines[idx + num_linhas].strip()
                    while "Priority =" not in prox_linha:
                        continuacao += prox_linha
                        num_linhas += 1
                        prox_linha = lines[idx + num_linhas].strip()
                    outputPath = Path(
                        (
                            line.split(JOB_WORKING_DIR_PATTERN)[1].strip()
                            + continuacao
                        ).split(":")[1]
                    )
                    workingDirectory = str(outputPath.parent)
                elif JOB_ARGS_PATTERN in line:
                    # Se atinge o tamanho máximo, pode continuar na linha
                    # seguinte. Continua até achar o padrão do próximo dado.
                    num_linhas = 1
                    continuacao = ""
                    prox_linha = lines[idx + num_linhas].strip()
                    while "start_time =" not in prox_linha:
                        continuacao += prox_linha
                        num_linhas += 1
                        prox_linha = lines[idx + num_linhas].strip()
                    args = (
                        line.split(JOB_ARGS_PATTERN)[1].strip() + continuacao
                    ).split(" ")
                    scriptFile = args[0]
                    jobArgs = args[1:]
                elif JOB_RESOURCE_PATTERN in line:
                    args = line.split(JOB_RESOURCE_PATTERN)[1].split("=")
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

            if len(jobs) != 1:
                return HTTPResponse(code=500, detail="")
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
            endTimeStr = "dequeuing from"
            resourcesStr = "Exit_status="
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
                elif endTimeStr in line:
                    endTime = datetime.strptime(
                        line[:19].strip(), "%m/%d/%Y %H:%M:%S"
                    )
            if startTime is None or endTime is None:
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
                workingDirectory=None,
                reservedSlots=int(slots),
                scriptFile=None,
                resourceUsage=usage,
                args=None,
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

        if not job.workingDirectory:
            return HTTPResponse(
                code=400, detail="workingDirectory is mandatory"
            )
        if not job.name:
            job.name = Path(job.workingDirectory).parts[-1]
        if not job.reservedSlots:
            return HTTPResponse(code=400, detail="reservedSlots is mandatory")
        if not job.scriptFile:
            return HTTPResponse(code=400, detail="scriptFile is mandatory")
        args = job.args if job.args is not None else []
        command = [
            "qsub",
            job.scriptFile,
            "-N",
            job.name,
            "-l",
            f"nodes={job.reservedSlots}",
            *args,
        ]
        if not isdir(job.workingDirectory):
            return HTTPResponse(
                code=400,
                detail=f"directory {job.workingDirectory} does not exist",
            )
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
            return Job(
                jobId=jobId,
                status=None,
                name=None,
                startTime=None,
                lastStatusUpdateTime=None,
                endTime=None,
                clusterId=Settings.clusterId,
                workingDirectory=None,
                reservedSlots=None,
                scriptFile=None,
                args=None,
                resourceUsage=None,
            )


class TestSchedulerRepository(AbstractSchedulerRepository):
    @staticmethod
    async def list_jobs() -> Union[List[Job], HTTPResponse]:
        return [
            Job(
                jobId="1",
                status=JobStatus.STARTING,
                name="teste",
                startTime=datetime(2024, 1, 1),
                lastStatusUpdateTime=datetime(2024, 1, 1),
                endTime=None,
                clusterId=Settings.clusterId,
                workingDirectory="/tmp",
                reservedSlots=64,
                scriptFile="/tmp/job.sh",
                args=None,
                resourceUsage=None,
            )
        ]

    @staticmethod
    async def get_job(jobId: str) -> Union[Job, HTTPResponse]:
        if jobId == "1":
            return Job(
                jobId="1",
                status=JobStatus.STARTING,
                name="teste",
                startTime=datetime(2024, 1, 1),
                lastStatusUpdateTime=datetime(2024, 1, 1),
                endTime=None,
                clusterId=Settings.clusterId,
                workingDirectory="/tmp",
                reservedSlots=64,
                scriptFile="/tmp/job.sh",
                args=None,
                resourceUsage=None,
            )
        else:
            return HTTPResponse(code=404, detail=f"job {jobId} not found")

    @staticmethod
    async def get_finished_job(jobId: str) -> Union[Job, HTTPResponse]:
        if jobId == "2":
            return Job(
                jobId="2",
                status=JobStatus.STOPPED,
                name="teste",
                startTime=datetime(2024, 1, 1),
                lastStatusUpdateTime=datetime(2024, 1, 1),
                endTime=None,
                clusterId=Settings.clusterId,
                workingDirectory="/tmp",
                reservedSlots=64,
                scriptFile="/tmp/job.sh",
                args=None,
                resourceUsage=None,
            )
        else:
            return HTTPResponse(code=404, detail=f"job {jobId} not found")

    @staticmethod
    async def submit_job(job: Job) -> Union[Job, HTTPResponse]:
        job.jobId = "3"
        return job

    @staticmethod
    async def stop_job(jobId: str) -> Union[Job, HTTPResponse]:
        return Job(
            jobId=jobId,
            status=JobStatus.STOPPING,
            name="teste",
            startTime=datetime(2024, 1, 1),
            lastStatusUpdateTime=datetime(2024, 1, 1),
            endTime=None,
            clusterId=Settings.clusterId,
            workingDirectory="/tmp",
            reservedSlots=64,
            scriptFile="/tmp/job.sh",
            args=None,
            resourceUsage=None,
        )


SUPPORTED_SCHEDULERS: Dict[str, Type[AbstractSchedulerRepository]] = {
    "SGE": SGESchedulerRepository,
    "TORQUE": TorqueSchedulerRepository,
    "TEST": TestSchedulerRepository,
}
DEFAULT = SGESchedulerRepository


def factory(kind: str) -> Type[AbstractSchedulerRepository]:
    return SUPPORTED_SCHEDULERS.get(kind, DEFAULT)
