import pytest
import pathlib
import os


# @pytest.fixture
# def test_settings():
BASEDIR = pathlib.Path().resolve()
os.environ["APP_INSTALLDIR"] = str(BASEDIR)
os.environ["APP_BASEDIR"] = str(BASEDIR)
os.environ["SCHEDULER"] = "TEST"
os.environ["PROGRAM_PATH_RULE"] = "TEST"
