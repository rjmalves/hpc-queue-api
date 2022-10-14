from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional

from app.models.jobstatus import JobStatus
from app.models.resourceusage import ResourceUsage


class Job(BaseModel):
    """
    Class for defining a compute fleet that runs inside a cluster,
    also known as a job in an HPC queue.
    """

    jobId: Optional[str]
    status: Optional[JobStatus]
    name: Optional[str]
    startTime: Optional[datetime]
    lastStatusUpdateTime: Optional[datetime]
    clusterId: str
    workingDirectory: Optional[str]
    reservedSlots: int
    scriptFile: Optional[str]
    args: Optional[List[str]]
    resourceUsage: Optional[ResourceUsage]
