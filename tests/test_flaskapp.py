import pytest
import time
from flaskapp import app

@pytest.fixture
def client():
    with app.app.test_client() as client:
        yield client

def test_valid_user_id(client):
    response = client.get('/recommend/123')

    assert response.status_code == 200

def test_invalid_user_id(client):
    response = client.get('recommend/abc')

    assert response.text == "invalid user ID"

def test_valid_return(client):
    time_start = time.time()
    response = client.get('/recommend/123')
    time_end = time.time()
    time_sum = time_end - time_start

    assert time_sum < 800


