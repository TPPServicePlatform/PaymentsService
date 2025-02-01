import pytest
from fastapi.testclient import TestClient
import os
import sys
import mongomock

# Run with the following command:
# pytest PaymentsService/api_container/tests/test_payments_api.py

# Set the TESTING environment variable
os.environ['TESTING'] = '1'
os.environ['MONGOMOCK'] = '1'

# Set a default MONGO_TEST_DB for testing
os.environ['MONGO_TEST_DB'] = 'test_db'

# Add the necessary paths to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'lib')))
from payments_api import app, coupons_manager

@pytest.fixture(scope='function')
def test_app():
    client = TestClient(app)
    yield client
    # Teardown: clear the database after each test
    coupons_manager.collection.drop()
