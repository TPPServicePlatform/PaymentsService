import pytest
import mongomock
from unittest.mock import patch
import sys
import os
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'lib')))
from coupons_nosql import Coupons

# Run with the following command:
# pytest PaymentsService/api_container/tests/test_coupons_nosql.py

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
def coupons(mongo_client):
    return Coupons(test_client=mongo_client)

def test_create_coupon(coupons, mocker):
    mocker.patch('lib.utils.get_actual_time', return_value='2023-01-01 00:00:00')
    success = coupons.insert(
        coupon_code= 'TEST_COUPON',
        discount_percent= 10,
        expiration_date= '2023-01-02 00:00:00'
    )
    assert success == True

def test_get_coupon(coupons, mocker):
    mocker.patch('lib.utils.get_actual_time', return_value='2023-01-01 00:00:00')
    success = coupons.insert(
        coupon_code= 'TEST_COUPON',
        discount_percent= 10,
        expiration_date= '2023-01-02 00:00:00'
    )
    assert success == True
    coupon = coupons.get('TEST_COUPON')
    assert coupon['uuid'] == 'TEST_COUPON'
    assert coupon['discount_percent'] == 10
    assert coupon['expiration_date'] == '2023-01-02 00:00:00'

def test_get_ineexistent_coupon(coupons):
    coupon = coupons.get('TEST_COUPON')
    assert coupon == None

def test_delete_coupon(coupons, mocker):
    mocker.patch('lib.utils.get_actual_time', return_value='2023-01-01 00:00:00')
    success = coupons.insert(
        coupon_code= 'TEST_COUPON',
        discount_percent= 10,
        expiration_date= '2023-01-02 00:00:00'
    )
    assert success == True
    success = coupons.delete('TEST_COUPON')
    assert success == True
    coupon = coupons.get('TEST_COUPON')
    assert coupon == None

def test_update_coupon(coupons, mocker):
    mocker.patch('lib.utils.get_actual_time', return_value='2023-01-01 00:00:00')
    success = coupons.insert(
        coupon_code= 'TEST_COUPON',
        discount_percent= 10,
        expiration_date= '2023-01-02 00:00:00'
    )
    assert success == True
    success = coupons.update('TEST_COUPON', {'discount_percent': 20})
    assert success == True
    coupon = coupons.get('TEST_COUPON')
    assert coupon['discount_percent'] == 20
    assert coupon['expiration_date'] == '2023-01-02 00:00:00'

def test_update_ineexistent_coupon(coupons):
    success = coupons.update('TEST_COUPON', {'discount_percent': 20})
    assert success == False
    coupon = coupons.get('TEST_COUPON')
    assert coupon == None

def test_add_user_to_coupon(coupons, mocker):
    mocker.patch('lib.utils.get_actual_time', return_value='2023-01-01 00:00:00')
    success = coupons.insert(
        coupon_code= 'TEST_COUPON',
        discount_percent= 10,
        expiration_date= '2023-01-02 00:00:00'
    )
    assert success == True
    success = coupons.add_user_to_coupon('TEST_COUPON', 'TEST_USER')
    coupons.print_all()
    assert success == True
    coupon = coupons.get('TEST_COUPON')
    assert 'TEST_USER' in coupon['used_by']

def test_add_user_to_ineexistent_coupon(coupons):
    success = coupons.add_user_to_coupon('TEST_COUPON', 'TEST_USER')
    assert success == False
    coupon = coupons.get('TEST_COUPON')
    assert coupon == None

def test_add_item_to_coupon(coupons, mocker):
    mocker.patch('lib.utils.get_actual_time', return_value='2023-01-01 00:00:00')
    success = coupons.insert(
        coupon_code= 'TEST_COUPON',
        discount_percent= 10,
        expiration_date= '2023-01-02 00:00:00',
        category_rules= ['TEST_CATEGORY_BETA']
    )
    assert success == True
    success = coupons.add_item_to_rule('TEST_COUPON', 'category_rules', 'TEST_CATEGORY')
    assert success == True
    coupon = coupons.get('TEST_COUPON')
    assert 'TEST_CATEGORY' in coupon['category_rules']