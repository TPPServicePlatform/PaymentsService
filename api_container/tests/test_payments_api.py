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

def test_create_coupon(test_app, mocker):
    mocker.patch('payments_api.get_actual_time', return_value='2023-01-01 00:00:00')
    body = {
        'coupon_code': 'TEST_COUPON',
        'discount_percent': 10.0,
        'expiration_date': "2023-01-31 23:59:59",
        'category_rules': ['category1', 'category2'],
    }
    response = test_app.post('/coupons/create', json=body)
    assert response.status_code == 200

    coupon = coupons_manager.get('TEST_COUPON')
    assert coupon is not None
    assert coupon['discount_percent'] == 10.0
    assert coupon['expiration_date'] == '2023-01-31 23:59:59'

def test_create_coupon_needs_rules(test_app, mocker):
    mocker.patch('payments_api.get_actual_time', return_value='2023-01-01 00:00:00')
    body = {
        'coupon_code': 'TEST_COUPON',
        'discount_percent': 10.0,
        'expiration_date': "2023-01-31 23:59:59"
    }
    response = test_app.post('/coupons/create', json=body)
    assert response.status_code == 400
    assert response.json()['detail'] == 'At least one rule is needed'

def test_delete_coupon(test_app, mocker):
    mocker.patch('payments_api.get_actual_time', return_value='2023-01-01 00:00:00')
    body = {
        'coupon_code': 'TEST_COUPON',
        'discount_percent': 10.0,
        'expiration_date': "2023-01-31 23:59:59",
        'category_rules': ['category1', 'category2'],
    }
    test_app.post('/coupons/create', json=body)

    response = test_app.delete('/coupons/delete/TEST_COUPON')
    assert response.status_code == 200

    coupon = coupons_manager.get('TEST_COUPON')
    assert coupon is None

def test_delete_coupon_not_found(test_app):
    response = test_app.delete('/coupons/delete/TEST_COUPON')
    assert response.status_code == 404
    assert response.json()['detail'] == 'Coupon not found'

def test_obtain_available_coupons(test_app, mocker):
    mocker.patch('payments_api.get_actual_time', return_value='2023-01-01 00:00:00')
    body = {
        'coupon_code': 'TEST_COUPON',
        'discount_percent': 10.0,
        'expiration_date': "2050-01-31 23:59:59",
        'category_rules': ['category1', 'category2'],
    }
    test_app.post('/coupons/create', json=body)

    response = test_app.get('/coupons', params={
        'user_id': 'test_user',
        'client_location': '10.0,20.0',
        'category': 'category1',
        'service_id': 'service1',
        'provider_id': 'provider1'
    })
    assert response.status_code == 200
    coupons = response.json().get('coupons')
    assert coupons is not None
    assert len(coupons) == 1
    assert coupons[0]['uuid'] == 'TEST_COUPON'

def test_obtain_available_coupons_no_coupons(test_app):
    response = test_app.get('/coupons', params={
        'user_id': 'test_user',
        'client_location': '10.0,20.0',
        'category': 'category1',
        'service_id': 'service1',
        'provider_id': 'provider1'
    })
    assert response.status_code == 200
    coupons = response.json().get('coupons')
    assert coupons is not None
    assert len(coupons) == 0

def test_activate_coupon(test_app, mocker):
    mocker.patch('payments_api.get_actual_time', return_value='2023-01-01 00:00:00')
    body = {
        'coupon_code': 'TEST_COUPON',
        'discount_percent': 10.0,
        'expiration_date': "2050-01-31 23:59:59",
        'category_rules': ['category1', 'category2'],
    }
    test_app.post('/coupons/create', json=body)

    body = {
        'client_location': '10.0,20.0',
        'category': 'category1',
        'service_id': 'service1',
        'provider_id': 'provider1'
    }
    response = test_app.put('/coupons/activate/TEST_COUPON/test_user', json=body)
    assert response.status_code == 200


