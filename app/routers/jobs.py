from fastapi import APIRouter, HTTPException, Depends
from typing import List
from app.internal.errorresponse import ErrorResponse
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
    ans = await scheduler.list_jobs()
    if isinstance(ans, ErrorResponse):
        raise HTTPException(status_code=ans.code, detail=ans.message)
    return ans


@router.post("/")
async def create_job(
    job: Job,
    scheduler: AbstractSchedulerRepository = Depends(scheduler),
) -> Job:

    ans = await scheduler.submit_job(job)
    if isinstance(ans, ErrorResponse):
        raise HTTPException(status_code=ans.code, detail=ans.message)
    return ans


@router.get("/{jobId}", response_model=Job)
async def read_job(
    jobId: str,
    scheduler: AbstractSchedulerRepository = Depends(scheduler),
) -> Job:
    ans = await scheduler.get_job(jobId)
    if isinstance(ans, ErrorResponse):
        raise HTTPException(status_code=ans.code, detail=ans.message)
    return ans


@router.delete("/{jobId}", response_model=Job)
async def delete_job(
    jobId: str,
    scheduler: AbstractSchedulerRepository = Depends(scheduler),
):
    ans = await scheduler.stop_job(jobId)
    if isinstance(ans, ErrorResponse):
        raise HTTPException(status_code=ans.code, detail=ans.message)
    return ans
