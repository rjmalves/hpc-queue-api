from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from typing import List
from app.internal.httpresponse import HTTPResponse
from app.models.job import Job

from app.adapters.schedulerrepository import AbstractSchedulerRepository
from app.internal.dependencies import scheduler

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"],
)

responses = {
    201: {"detail": ""},
    202: {"detail": ""},
    404: {"detail": ""},
    500: {"detail": ""},
    503: {"detail": ""},
}


@router.get("/", response_model=List[Job], responses=responses)
async def read_jobs(
    scheduler: AbstractSchedulerRepository = Depends(scheduler),
):
    ans = await scheduler.list_jobs()
    if isinstance(ans, HTTPResponse):
        raise HTTPException(status_code=ans.code, detail=ans.detail)
    return ans


@router.post("/", responses=responses)
async def create_job(
    job: Job,
    scheduler: AbstractSchedulerRepository = Depends(scheduler),
):

    ans = await scheduler.submit_job(job)
    if isinstance(ans, HTTPResponse):
        raise HTTPException(status_code=ans.code, detail=ans.detail)
    return JSONResponse(
        status_code=201, content={"detail": f"jobId: {ans.jobId}"}
    )


@router.get("/{jobId}", response_model=Job, responses=responses)
async def read_job(
    jobId: str,
    scheduler: AbstractSchedulerRepository = Depends(scheduler),
):
    ans = await scheduler.get_job(jobId)
    if isinstance(ans, HTTPResponse):
        ans_finished = await scheduler.get_finished_job(jobId)
        if not isinstance(ans_finished, HTTPResponse):
            return ans_finished
        raise HTTPException(status_code=ans.code, detail=ans.detail)
    return ans


@router.delete("/{jobId}", responses=responses)
async def delete_job(
    jobId: str,
    scheduler: AbstractSchedulerRepository = Depends(scheduler),
):
    ans = await scheduler.stop_job(jobId)
    if isinstance(ans, HTTPResponse):
        raise HTTPException(status_code=ans.code, detail=ans.detail)
    return JSONResponse(status_code=202, content={"detail": f"jobId: {jobId}"})
