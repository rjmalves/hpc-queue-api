from dotenv import load_dotenv
import os
import pathlib
from fastapi import FastAPI
from app.routers import jobs
from app.internal.settings import Settings

BASEDIR = pathlib.Path().resolve()
os.environ["APP_INSTALLDIR"] = os.path.dirname(os.path.abspath(__file__))
load_dotenv(
    pathlib.Path(os.getenv("APP_INSTALLDIR")).joinpath(".env"),
    override=True,
)
Settings.read_environments()

app = FastAPI()

app.include_router(jobs.router)
