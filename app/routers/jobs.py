from fastapi import APIRouter, HTTPException, Depends
from typing import List
from app.internal.settings import Settings
from app.models.job import Job

from app.adapters.schedulerrepository import AbstractSchedulerRepository
from app.internal.dependencies import scheduler

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"],
)


@router.get("/", response_model=List[Job])
async def read_jobs(
    scheduler: AbstractSchedulerRepository = Depends(scheduler),
) -> List[Job]:
    jobs = await scheduler.list_jobs()
    if jobs is None:
        raise HTTPException(
            status_code=500, detail="error listing existing jobs"
        )
    return jobs


@router.post("/")
async def create_job(
    job: Job,
    scheduler: AbstractSchedulerRepository = Depends(scheduler),
) -> Job:
    submitted_job = await scheduler.submit_job(job)
    if submitted_job is None:
        raise HTTPException(status_code=500, detail="error submitting job")
    return submitted_job


@router.get("/{jobId}", response_model=Job)
async def read_job(
    jobId: str,
    scheduler: AbstractSchedulerRepository = Depends(scheduler),
) -> Job:
    job = await scheduler.get_job(jobId)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@router.delete("/{jobId}", response_model=Job)
async def delete_job(
    jobId: str,
    scheduler: AbstractSchedulerRepository = Depends(scheduler),
):
    job = await scheduler.stop_job(jobId)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job
