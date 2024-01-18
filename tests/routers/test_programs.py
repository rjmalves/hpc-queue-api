from app.app import make_app
from fastapi.testclient import TestClient

app = make_app()
client = TestClient(app)


def test_get_programs():
    response = client.get("/programs/")
    assert response.status_code == 200
    assert len(response.json()) == 2
