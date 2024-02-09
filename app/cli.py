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
@click.option("-j", "--jobid", default=None, help="id for the job")
def list_jobs(jobid):
    """
    List jobs running and waiting in the HPC queue.
    """

    def __trim_path(path: str) -> str:
        """
        Trim the path to the last 30 characters.
        """
        max_path_size = 40
        if len(path) > max_path_size:
            return "..." + path[-max_path_size:]
        return path

    if jobid is None:
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
    else:
        url = f"http://localhost:{Settings.port}/{Settings.root_path}jobs/{jobid}"
        res = requests.get(url)
        jobs = res.json()
        print(jobs)


@click.command("submit-job")
@click.argument(
    "scriptfile",
    type=click.Path(
        exists=True,
        file_okay=True,
        readable=True,
        executable=True,
        resolve_path=True,
    ),
)
@click.argument(
    "slots",
    type=int,
)
@click.argument(
    "args",
    nargs=-1,
)
@click.option("-n", "--name", default=None, help="name for the job")
@click.option(
    "-wd", "--workdir", default=None, help="working directory for the job"
)
def submit_job(scriptfile, slots, args, name, workdir):
    """
    Submit jobs in the HPC queue.
    """
    if slots <= 0:
        raise ValueError("slots <= 0")
    if workdir is None:
        workdir = str(Path(curdir).resolve())
    if name is None:
        name = Path(workdir).parts[-1]

    url = f"http://localhost:{Settings.port}/{Settings.root_path}jobs/"
    job = Job(
        jobId=None,
        status=None,
        name=name,
        startTime=None,
        lastStatusUpdateTime=None,
        endTime=None,
        clusterId=Settings.clusterId,
        workingDirectory=workdir,
        reservedSlots=slots,
        scriptFile=str(scriptfile),
        args=list(args),
        resourceUsage=None,
    )
    res = requests.post(url, json=job.model_dump())

    if res.status_code != 201:
        raise RuntimeError(res.json())

    content = res.json()
    print(f"Your job {name} ({content['jobId']}) was submitted.")


@click.command("delete-job")
@click.argument(
    "jobid",
    type=int,
)
def delete_job(jobid):
    """
    Delete jobs in the HPC queue.
    """

    url = f"http://localhost:{Settings.port}/{Settings.root_path}jobs/{jobid}"
    res = requests.delete(url)

    if res.status_code != 202:
        print(res)
        raise RuntimeError(res.json())

    content = res.json()
    jobId = content["detail"].split("jobId:")[1].strip()
    print(f"Your job ({jobId}) was deleted.")


cli.add_command(list_jobs)
cli.add_command(submit_job)
cli.add_command(delete_job)
