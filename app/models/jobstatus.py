from enum import Enum


class JobStatus(Enum):
    START_REQUESTED = "START_REQUESTED"
    STARTING = "STARTING"
    ENABLED = "ENABLED"
    RUNNING = "RUNNING"
    PROTECTED = "PROTECTED"
    DISABLED = "DISABLED"
    STOP_REQUESTED = "STOP_REQUESTED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    UNKNOWN = "UNKNOWN"

    @staticmethod
    def factory(s: str) -> "JobStatus":
        for status in JobStatus:
            if status.value == s:
                return status
        return JobStatus.UNKNOWN
