from app.cli import cli
import os
import pathlib
from dotenv import load_dotenv
from app.internal.settings import Settings


def main():
    os.environ["APP_INSTALLDIR"] = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(
        pathlib.Path(os.getenv("APP_INSTALLDIR")).joinpath(".env"),
        override=True,
    )
    Settings.read_environments()
    cli()


if __name__ == "__main__":
    main()
