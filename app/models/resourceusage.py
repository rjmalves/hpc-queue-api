from datetime import datetime
from pydantic import BaseModel


class ResourceUsage(BaseModel):
    """
    Class for storing the computation resource usage of a job / process.
    """

    cpu: float
    memory: float
    io: float
    timeInstant: datetime
