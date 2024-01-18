from fastapi import FastAPI
from app.routers import jobs, programs


def make_app(root_path: str = "/") -> FastAPI:
    app = FastAPI(root_path=root_path)
    app.include_router(jobs.router)
    app.include_router(programs.router)
    return app
