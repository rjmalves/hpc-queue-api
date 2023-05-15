from app.internal.settings import Settings
from fastapi import HTTPException
from app.adapters.schedulerrepository import AbstractSchedulerRepository
from app.adapters.schedulerrepository import factory as scheduler_factory
from app.adapters.programpathrepository import AbstractProgramPathRepository
from app.adapters.programpathrepository import factory as programs_factory
from typing import Type


async def scheduler() -> Type[AbstractSchedulerRepository]:
    s = scheduler_factory(Settings.scheduler)
    if s is None:
        raise HTTPException(
            500, f"scheduler {Settings.scheduler} not supported"
        )
    return s


async def programPath() -> Type[AbstractProgramPathRepository]:
    s = programs_factory(Settings.programPathRule)
    if s is None:
        raise HTTPException(
            500, f"programPath {Settings.programPathRule} not supported"
        )
    return s
