from fastapi import APIRouter, HTTPException
from typing import List
from app.internal.settings import Settings
from app.models.job import Job

from app.adapters.schedulerrepository import factory as scheduler_factory

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"],
)


@router.get("/")
async def read_jobs() -> List[Job]:
    scheduler = scheduler_factory(Settings.scheduler)
    jobs = await scheduler.list_jobs()
    if jobs is None:
        raise HTTPException(
            status_code=500, detail="error listing existing jobs"
        )
    return jobs


@router.post("/")
async def create_job(job: Job) -> Job:
    try:
        scheduler = scheduler_factory(Settings.scheduler)
    except RuntimeError:
        raise HTTPException(status_code=500)
    return await scheduler.submit_job(job)


@router.get("/{jobId}")
async def read_job(jobId: str) -> Job:
    try:
        scheduler = scheduler_factory(Settings.scheduler)
    except RuntimeError:
        raise HTTPException(status_code=500)
    return await scheduler.get_job(jobId)


@router.delete("/{jobId}")
async def delete_job(jobId: str):
    try:
        scheduler = scheduler_factory(Settings.scheduler)
    except RuntimeError:
        raise HTTPException(status_code=500)
    return await scheduler.stop_job(jobId)
