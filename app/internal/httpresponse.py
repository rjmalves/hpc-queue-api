from pydantic import BaseModel


class HTTPResponse(BaseModel):
    code: int
    message: str
