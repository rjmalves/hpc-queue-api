import asyncio
from typing import Dict, Any
from os import chdir
from datetime import datetime
from app.models.job import Job
from app.models.jobstatus import JobStatus
from app.utils.singleton import Singleton
from app.internal.terminal import run_terminal_retry
from app.internal.settings import Settings


class TaskScheduler(metaclass=Singleton):
    TASKS: Dict[str, asyncio.Task] = dict()
    JOBS: Dict[str, Job] = dict()
    MAX_SLOTS = Settings.max_slots

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
            timeout = 60 * 60 * 24 * 7  # 7 days
            cls.jobs()[job.jobId].status = JobStatus.START_REQUESTED
            while True:
                if cls.free_slots() >= job.reservedSlots:
                    break
                await asyncio.sleep(5)
            cls.jobs()[job.jobId].status = JobStatus.RUNNING
            cls.jobs()[job.jobId].startTime = datetime.now()
            await run_terminal_retry(
                [job.scriptFile] + job.args, timeout=timeout
            )

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
