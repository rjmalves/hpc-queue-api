import asyncio
import signal
from typing import Dict, Any, List
from os import chdir
from datetime import datetime
from app.models.job import Job
from app.models.jobstatus import JobStatus
from app.utils.singleton import Singleton
from app.internal.settings import Settings
from app.internal.terminal import run_terminal


class TaskScheduler(metaclass=Singleton):
    TASKS: Dict[str, asyncio.Task] = dict()
    JOBS: Dict[str, Job] = dict()
    MAX_SLOTS = Settings.max_slots
    MAX_KILL_RETRY = 10

    @classmethod
    def tasks(cls) -> Dict[str, asyncio.Task]:
        return cls.TASKS

    @classmethod
    def jobs(cls) -> Dict[str, Job]:
        return cls.JOBS

    @classmethod
    def free_slots(cls) -> int:
        used_slots = 0
        for k, _ in cls.tasks().items():
            job = cls.jobs()[k]
            if job.status == JobStatus.RUNNING:
                if isinstance(job.reservedSlots, int):
                    used_slots += job.reservedSlots
        return cls.MAX_SLOTS - used_slots

    @classmethod
    def _remove_from_dict_by_value(cls, value: asyncio.Task[Any]) -> None:

        for k, v in cls.tasks().items():
            if v == value:
                cls.jobs()[k].status = JobStatus.STOPPED
                cls.jobs()[k].lastStatusUpdateTime = datetime.now()
                cls.jobs()[k].endTime = datetime.now()
                cls.tasks().pop(k)
                break

    @classmethod
    def schedule_task(cls, job: Job):

        async def _cmd(
            command: str, args: List[str], outfile: str, errfile: str
        ) -> None:
            try:
                with open(outfile, "w") as outfile:
                    with open(errfile, "w") as errfile:
                        proc = await asyncio.create_subprocess_exec(
                            command,
                            args,
                            stdout=outfile,
                            stderr=errfile,
                        )
                        await proc.communicate()
                        print(proc.pid)
            except asyncio.CancelledError:
                pid = proc.pid
                for _ in range(cls.MAX_KILL_RETRY):
                    await asyncio.sleep(1)
                    print(f"trying to kill process [{pid}]...")
                    cod, ans = await run_terminal([f"kill -9 {pid}"])
                    print(cod, ans)
                    # DESSEM only exits by doing this manually
                    if cod == 1:
                        print("process killed!")
                        break

        async def task(job: Job) -> None:
            if not job.workingDirectory:
                raise ValueError("Working directory is not set.")
            if not job.reservedSlots:
                raise ValueError("Reserved slots is not set.")
            if not job.jobId:
                raise ValueError("Job ID is not set.")
            if not job.scriptFile:
                raise ValueError("Script file is not set.")
            chdir(job.workingDirectory)
            timeout = 60 * 60 * 24 * 7
            cls.jobs()[job.jobId].status = JobStatus.START_REQUESTED
            cls.jobs()[taskid].lastStatusUpdateTime = datetime.now()
            try:
                while True:
                    if cls.free_slots() >= job.reservedSlots:
                        break
                    await asyncio.sleep(5)
                cls.jobs()[job.jobId].status = JobStatus.RUNNING
                cls.jobs()[job.jobId].startTime = datetime.now()
                outfile = f"{job.name}.o{job.jobId}"
                errfile = f"{job.name}.e{job.jobId}"
                await asyncio.wait_for(
                    _cmd(job.scriptFile, " ".join(job.args), outfile, errfile),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                print(f"job timeout error: {job.jobId}")

        taskids = [int(i) for i in list(cls.jobs().keys())]
        if len(taskids) == 0:
            job.jobId = "1"
        else:
            job.jobId = str(max(taskids) + 1)
        taskid = job.jobId
        cls.jobs()[taskid] = job
        ref: asyncio.Task = asyncio.create_task(task(job), name=taskid)
        cls.tasks()[taskid] = ref
        ref.add_done_callback(cls._remove_from_dict_by_value)

    @classmethod
    def kill_task(cls, taskid: str) -> bool:
        if taskid in cls.tasks():
            cls.jobs()[taskid].status = JobStatus.STOP_REQUESTED
            cls.jobs()[taskid].lastStatusUpdateTime = datetime.now()
            res = cls.tasks()[taskid].cancel()
            return res
        else:
            return False
