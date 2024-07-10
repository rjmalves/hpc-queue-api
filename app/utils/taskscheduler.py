import asyncio
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

        async def _get_process(pid: int) -> int:
            cod, _ = await run_terminal(
                [f"ps -p {pid} -o command"], timeout=10
            )
            return cod

        async def _cmd(
            command: str,
            args: List[str],
            workdir: str,
            outfile: str,
            errfile: str,
        ) -> None:
            chdir(workdir)
            try:
                with open(outfile, "w") as outfile:
                    with open(errfile, "w") as errfile:
                        proc = await asyncio.create_subprocess_exec(
                            command,
                            *args,
                            stdout=outfile,
                            stderr=errfile,
                        )
                        await proc.communicate()
            except asyncio.CancelledError:
                pid = proc.pid
                # DESSEM only exits by doing this manually
                while await _get_process(pid) == 0:
                    await asyncio.sleep(1)
                    await run_terminal([f"pkill -9 -P {pid}"])

        def _is_next(job: Job) -> bool:
            # Checks if the job is the next in the queue
            waiting_jobs = {
                jobId: j
                for jobId, j in cls.jobs().items()
                if j.status == JobStatus.START_REQUESTED
            }
            # Checks if there are enough slots
            waiting_jobs_with_enough_slots = {
                jobId: j
                for jobId, j in waiting_jobs.items()
                if cls.free_slots() >= j.reservedSlots
            }
            # If the first submitted job, with enough slots, is the current
            # job, then it is the next in the queue
            sorted_waiting_jobs = sorted(
                waiting_jobs_with_enough_slots.items(),
                key=lambda x: x[1].lastStatusUpdateTime,
            )
            if len(sorted_waiting_jobs) == 0:
                return False
            else:
                return list(sorted_waiting_jobs)[0][0] == job.jobId

        async def task(job: Job) -> None:
            if not job.workingDirectory:
                raise ValueError("Working directory is not set.")
            if not job.reservedSlots:
                raise ValueError("Reserved slots is not set.")
            if not job.jobId:
                raise ValueError("Job ID is not set.")
            if not job.scriptFile:
                raise ValueError("Script file is not set.")

            timeout = 60 * 60 * 24 * 7
            cls.jobs()[job.jobId].status = JobStatus.START_REQUESTED
            cls.jobs()[taskid].lastStatusUpdateTime = datetime.now()
            try:
                while True:
                    if _is_next(job):
                        break
                    await asyncio.sleep(5)
                cls.jobs()[job.jobId].status = JobStatus.RUNNING
                cls.jobs()[job.jobId].startTime = datetime.now()
                outfile = f"{job.name}.o{job.jobId}"
                errfile = f"{job.name}.e{job.jobId}"
                await asyncio.wait_for(
                    _cmd(
                        job.scriptFile,
                        job.args,
                        job.workingDirectory,
                        outfile,
                        errfile,
                    ),
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
