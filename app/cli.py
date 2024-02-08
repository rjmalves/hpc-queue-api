import click
import requests
import pandas as pd
from tabulate import tabulate
from app.internal.settings import Settings


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


# @click.command("submit-job")
# def submit_job():
#     """
#     List jobs running and waiting in the HPC queue.
#     """
#     # Obter URL em que a API está hospedada
#     url = f"http://localhost:{Settings.port}/{Settings.root_path}jobs/"
#     # Fazer requisição para a rota de listar jobs
#     res = requests.get(url)
#     # Obter resultado e imprimir na tela
#     # Tabela com
#     # ID | Nome | Status | Instante de criação | Slots
#     jobs = res.json()
#     df = pd.DataFrame.from_records(jobs)[
#         ["jobId", "name", "status", "startTime", "reservedSlots"]
#     ]
#     df["startTime"] = pd.to_datetime(df["startTime"])
#     df["startTime"] = df["startTime"].dt.strftime("%H:%M:%S %d-%m-%Y")
#     df["startTime"] = df["startTime"].fillna("")
#     print(tabulate(df, headers="keys", showindex=False))


cli.add_command(list_jobs)
