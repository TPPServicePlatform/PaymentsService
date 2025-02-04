import pytest
import mongomock
from unittest.mock import patch
import sys
import os
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'lib')))
from loyalty_nosql import Loyalty, get_actual_time, EXPIRED_POINTS_MESSAGE

# Run with the following command:
# pytest PaymentsService/api_container/tests/test_loyalty_nosql.py

# Set the TESTING environment variable
os.environ['TESTING'] = '1'
os.environ['MONGOMOCK'] = '1'

# Set a default MONGO_TEST_DB for testing
os.environ['MONGO_TEST_DB'] = 'test_db'

@pytest.fixture(scope='function')
def mongo_client():
    client = mongomock.MongoClient()
    yield client
    client.drop_database(os.getenv('MONGO_TEST_DB'))
    client.close()

@pytest.fixture(scope='function')
def loyalty(mongo_client):
    return Loyalty(test_client=mongo_client)

def test_create_user_doc(loyalty, mocker):
    success = loyalty._create_user_doc('user_id')
    assert success == True

def test_get_total_points(loyalty, mocker):
    success = loyalty._create_user_doc('user_id')
    assert success == True

    points = loyalty.get_total_points('user_id')
    assert points == 0

def test_add_transaction(loyalty, mocker):
    success = loyalty.add_transaction('user_id', 100, 'Test positive transaction')
    assert success == True

    points = loyalty.get_total_points('user_id')
    assert points == 100

def test_add_negative_transaction(loyalty, mocker):
    success = loyalty.add_transaction('user_id', 100, 'Test positive transaction')
    assert success == True

    success = loyalty.add_transaction('user_id', -50, 'Test negative transaction')
    assert success == True

    points = loyalty.get_total_points('user_id')
    assert points == 50

def test_add_multiple_negative_transactions(loyalty, mocker):
    success = loyalty.add_transaction('user_id', 100, 'Test positive transaction')
    assert success == True

    success = loyalty.add_transaction('user_id', -50, 'Test negative transaction')
    assert success == True

    success = loyalty.add_transaction('user_id', -60, 'Test negative transaction')
    assert success == False

    points = loyalty.get_total_points('user_id')
    assert points == 50

def test_expiring_transactions(loyalty, mocker):
    mocker.patch('loyalty_nosql.get_actual_time', return_value='2025-01-01')
    success = loyalty.add_transaction('user_id', 100, 'Test positive transaction')
    assert success == True

    success = loyalty.add_transaction('user_id', 100, 'Test positive transaction')
    assert success == True

    mocker.patch('loyalty_nosql.get_actual_time', return_value='2028-01-01')
    points = loyalty.get_total_points('user_id')
    assert points == 0

def test_use_expiring_points(loyalty, mocker):
    mocker.patch('loyalty_nosql.get_actual_time', return_value='2025-01-01')
    success = loyalty.add_transaction('user_id', 100, 'Test positive transaction')
    assert success == True

    success = loyalty.add_transaction('user_id', 100, 'Test positive transaction')
    assert success == True

    mocker.patch('loyalty_nosql.get_actual_time', return_value='2028-01-01')
    points = loyalty.get_total_points('user_id')
    assert points == 0

    success = loyalty.add_transaction('user_id', -50, 'Test negative transaction')
    assert success == False

def test_get_history(loyalty, mocker):
    mocker.patch('loyalty_nosql.get_actual_time', return_value='2023-01-01')
    mocker.patch('loyalty_nosql.get_timestamp_after_days', return_value='2025-01-01')
    success = loyalty.add_transaction('user_id', 100, 'Test positive transaction')
    assert success == True

    mocker.patch('loyalty_nosql.get_actual_time', return_value='2023-06-01')
    mocker.patch('loyalty_nosql.get_timestamp_after_days', return_value='2025-06-01')
    success = loyalty.add_transaction('user_id', -50, 'Test negative transaction')
    assert success == True

    mocker.patch('loyalty_nosql.get_actual_time', return_value='2025-01-01')
    mocker.patch('loyalty_nosql.get_timestamp_after_days', return_value='2027-01-01')
    success = loyalty.add_transaction('user_id', -50, 'Test negative transaction')
    assert success == False

    success = loyalty.add_transaction('user_id', 100, 'Test positive transaction')
    assert success == True

    history = loyalty.get_history('user_id')
    assert len(history) == 4

    assert {'points': 100, 'timestamp': '2023-01-01', 'description': 'Test positive transaction'} in history
    assert {'points': -50, 'timestamp': '2023-06-01', 'description': 'Test negative transaction'} in history
    assert {'points': -50, 'timestamp': '2025-01-01', 'description': EXPIRED_POINTS_MESSAGE} in history
    assert {'points': 100, 'timestamp': '2025-01-01', 'description': 'Test positive transaction'} in history

def test_get_expiring_points(loyalty, mocker):
    mocker.patch('loyalty_nosql.get_actual_time', return_value='2023-01-01')
    mocker.patch('loyalty_nosql.get_timestamp_after_days', return_value='2025-01-01')
    success = loyalty.add_transaction('user_id', 100, 'Test positive transaction')
    assert success == True

    expiring_points = loyalty.get_expiring_points('user_id')
    assert len(expiring_points) == 1
    assert {'points': 100, 'expiration_date': '2025-01-01'} in expiring_points

    mocker.patch('loyalty_nosql.get_actual_time', return_value='2023-06-01')
    mocker.patch('loyalty_nosql.get_timestamp_after_days', return_value='2025-06-01')
    success = loyalty.add_transaction('user_id', -50, 'Test negative transaction')
    assert success == True

    expiring_points = loyalty.get_expiring_points('user_id')
    assert len(expiring_points) == 1
    assert {'points': 50, 'expiration_date': '2025-01-01'} in expiring_points

    mocker.patch('loyalty_nosql.get_actual_time', return_value='2025-01-01')
    mocker.patch('loyalty_nosql.get_timestamp_after_days', return_value='2027-01-01')
    expiring_points = loyalty.get_expiring_points('user_id')
    assert len(expiring_points) == 0

    success = loyalty.add_transaction('user_id', -50, 'Test negative transaction')
    assert success == False

    success = loyalty.add_transaction('user_id', 100, 'Test positive transaction')
    assert success == True

    expiring_points = loyalty.get_expiring_points('user_id')
    assert len(expiring_points) == 1
    assert {'points': 100, 'expiration_date': '2027-01-01'} in expiring_points