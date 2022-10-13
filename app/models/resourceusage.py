from datetime import datetime
from pydantic import BaseModel


class ResourceUsage(BaseModel):
    """
    Class for storing the computation resource usage of a job / process.
    """

    cpuSeconds: float
    memoryCpuSeconds: float
    instantTotalMemory: float
    maxTotalMemory: float
    processIO: float
    processIOWaiting: float
    timeInstant: datetime
