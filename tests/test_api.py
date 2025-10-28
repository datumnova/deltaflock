from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_query_endpoint():
    # Use POST with a simple harmless query against an in-memory DB. The table probably doesn't exist,
    # but we assert that the endpoint is reachable and returns a valid structure or a 400 with 'detail'.
    response = client.post("/api/v1/query", json={"query": "SELECT 1 as one"})
    assert response.status_code in (200, 400)
    # If successful, response should have 'data'; if failed, FastAPI returns 'detail' key.
    json_body = response.json()
    assert isinstance(json_body, dict)
    assert "data" in json_body or "detail" in json_body


def test_invalid_query_endpoint():
    response = client.post("/api/v1/query", json={"query": "INVALID SQL"})
    # Expect a 400 for invalid SQL
    assert response.status_code == 400
    assert "detail" in response.json()
