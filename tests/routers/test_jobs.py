from app.routers.jobs import router
from fastapi.testclient import TestClient
from fastapi import HTTPException
import pytest

client = TestClient(router)


def test_get_jobs():
    response = client.get("/jobs/")
    assert response.status_code == 200
    assert len(response.json()) == 1


def test_get_job_not_found():
    with pytest.raises(HTTPException):
        response = client.get("/jobs/0")
        assert response.status_code == 404


def test_get_job_running():
    response = client.get("/jobs/1")
    assert response.status_code == 200
    job = response.json()
    assert job["jobId"] == "1"
    assert job["status"] == "STARTING"
    assert job["name"] == "teste"
    assert job["startTime"] == "2024-01-01T00:00:00"
    assert job["lastStatusUpdateTime"] == "2024-01-01T00:00:00"
    assert job["endTime"] == None
    assert job["workingDirectory"] == "/tmp"
    assert job["reservedSlots"] == 64
    assert job["scriptFile"] == "/tmp/job.sh"
    assert job["args"] == None
    assert job["resourceUsage"] == None


def test_get_job_finished():
    response = client.get("/jobs/2")
    assert response.status_code == 200
    job = response.json()
    assert job["jobId"] == "2"
    assert job["status"] == "STOPPED"
    assert job["name"] == "teste"
    assert job["startTime"] == "2024-01-01T00:00:00"
    assert job["lastStatusUpdateTime"] == "2024-01-01T00:00:00"
    assert job["endTime"] == None
    assert job["workingDirectory"] == "/tmp"
    assert job["reservedSlots"] == 64
    assert job["scriptFile"] == "/tmp/job.sh"
    assert job["args"] == None
    assert job["resourceUsage"] == None


def test_post_job():
    job = {
        "jobId": None,
        "status": None,
        "name": "teste",
        "startTime": None,
        "lastStatusUpdateTime": None,
        "endTime": None,
        "clusterId": "0",
        "workingDirectory": "/tmp",
        "reservedSlots": 64,
        "scriptFile": "/tmp/job.sh",
        "args": ["64"],
        "resourceUsage": None,
    }
    response = client.post("/jobs/", json=job)
    assert response.status_code == 201
    res = response.json()
    assert res["jobId"] == "3"


def test_stop_job():
    response = client.delete("/jobs/3")
    assert response.status_code == 202
    res = response.json()
    assert res["detail"] == "jobId: 3"
