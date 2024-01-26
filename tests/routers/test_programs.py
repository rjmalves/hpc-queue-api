from app.routers.programs import router
from fastapi.testclient import TestClient

client = TestClient(router)


def test_get_programs():
    response = client.get("/programs/")
    assert response.status_code == 200
    assert len(response.json()) == 2
