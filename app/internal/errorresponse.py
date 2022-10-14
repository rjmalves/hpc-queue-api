from dataclasses import dataclass


@dataclass
class ErrorResponse:
    code: int
    message: str
