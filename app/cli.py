import click
import requests
import pandas as pd
from tabulate import tabulate
from app.internal.settings import Settings
from app.models.job import Job
from app.models.jobstatus import JobStatus
from os import curdir
from pathlib import Path


@click.group()
def cli():
    """
    CLI interface for dealing with the HPC queue and its jobs.
    """
    pass


@click.command("list-jobs")
def list_jobs():
    """
    List jobs running and waiting in the HPC queue.
    """

    def __trim_path(path: str) -> str:
        """
        Trim the path to the last 30 characters.
        """
        max_path_size = 20
        if len(path) > max_path_size:
            return "..." + path[-max_path_size:]
        return path

    url = f"http://localhost:{Settings.port}/{Settings.root_path}jobs/"
    res = requests.get(url)
    jobs = res.json()
    df = pd.DataFrame.from_records(jobs)
    if not df.empty:
        df = df[
            [
                "jobId",
                "name",
                "status",
                "startTime",
                "reservedSlots",
                "workingDirectory",
            ]
        ]
        df = df.rename(columns={"jobId": "id", "reservedSlots": "slots"})
        df["startTime"] = pd.to_datetime(df["startTime"])
        df["startTime"] = df["startTime"].dt.strftime("%H:%M:%S %d-%m-%Y")
        df["startTime"] = df["startTime"].fillna("")
        df["workingDirectory"] = df["workingDirectory"].apply(__trim_path)
    print(tabulate(df, headers="keys", showindex=False))


@click.command("submit-job")
@click.argument(
    "scriptFile",
    type=click.Path(
        exists=True,
        file_okay=True,
        readable=True,
        executable=True,
        resolve_path=True,
    ),
)
@click.argument(
    "reservedSlots",
    type=int,
)
@click.option("--name", default=None, help="name for the job")
@click.option("--workdir", default=None, help="working directory for the job")
def submit_job(scriptFile, reservedSlots, name, workdir):
    """
    Submit jobs in the HPC queue.
    """
    if reservedSlots <= 0:
        raise ValueError("reservedSlots <= 0")
    if workdir is None:
        workdir = str(Path(curdir).resolve())
    if name is None:
        name = Path(workdir).parts[-1]

    url = f"http://localhost:{Settings.port}/{Settings.root_path}jobs/"
    args = []
    job = Job(
        jobId=None,
        status=None,
        name=name,
        startTime=None,
        lastStatusUpdateTime=None,
        endTime=None,
        clusterId=Settings.clusterId,
        workingDirectory=workdir,
        reservedSlots=reservedSlots,
        scriptFile=str(scriptFile),
        args=args,
        resourceUsage=None,
    )
    res = requests.post(url, json=job.model_dump_json())

    if res.status_code != 201:
        raise RuntimeError(res)

    content = res.json()
    print(f"Your job {name} ({content['jobId']}) was submitted.")


cli.add_command(list_jobs)
