from dotenv import load_dotenv
import uvicorn
import os
import pathlib
from app.internal.settings import Settings
from app.app import make_app

BASEDIR = pathlib.Path().resolve()
os.environ["APP_INSTALLDIR"] = os.path.dirname(os.path.abspath(__file__))
load_dotenv(
    pathlib.Path(os.getenv("APP_INSTALLDIR")).joinpath(".env"),
    override=True,
)
Settings.read_environments()
app = make_app(root_path=Settings.root_path)


if __name__ == "__main__":
    uvicorn.run(
        "main:app", host=Settings.host, port=Settings.port, log_level="info"
    )
