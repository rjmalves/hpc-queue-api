from datetime import datetime
from pydantic import BaseModel


# SGE:
# Uso de CPU, MEM e IO:
# - CPU: valores acumulados em segundos
# - MEM: valores acumulados de uso de memória em GB * segundos de CPU
# - IO:  valores acumulados em quantidade de dados transferidos
#  - IOW: valores de espera acumulados para IO em segundos
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
