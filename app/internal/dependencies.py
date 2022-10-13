from app.internal.settings import Settings
from fastapi import HTTPException
from app.adapters.schedulerrepository import AbstractSchedulerRepository
from app.adapters.schedulerrepository import factory as scheduler_factory


async def scheduler() -> AbstractSchedulerRepository:
    s = scheduler_factory(Settings.scheduler)
    if s is None:
        raise HTTPException(
            500, f"scheduler {Settings.scheduler} not supported"
        )
    return s
