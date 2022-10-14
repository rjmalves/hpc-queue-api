from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional
from app.models.program import Program

from app.adapters.programpathrepository import AbstractProgramPathRepository
from app.internal.dependencies import programPath

router = APIRouter(
    prefix="/programs",
    tags=["programs"],
)


@router.get("/", response_model=List[Program])
async def read_programs(
    name: Optional[str] = None,
    version: Optional[str] = None,
    programPath: AbstractProgramPathRepository = Depends(programPath),
) -> List[Program]:
    programs = await programPath.list_programs()
    if programs is None:
        raise HTTPException(
            status_code=500, detail="error listing existing programs"
        )
    if name:
        programs = [p for p in programs if p.name == name]
    if version:
        programs = [p for p in programs if p.version == version]
    return programs
