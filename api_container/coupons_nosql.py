from typing import Optional, List, Dict
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError, OperationFailure
import logging as logger
import os
import sys
import uuid

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'lib')))
from lib.utils import get_actual_time, get_mongo_client

HOUR = 60 * 60
MINUTE = 60
MILLISECOND = 1_000

# TODO: (General) -> Create tests for each method && add the required checks in each method
class Coupons:
    """
    Coupons class that stores data in a MongoDB collection.
    Fields:
    - id: int (unique) [pk]
    - coupon_code: str (unique)
    - discount_percent: float
    - max_discount: float (optional)
    - expiration_date: datetime
    - created_at: datetime
    - updated_at: datetime
    - used_by: List[str] -> List of user ids that used the coupon, the idea is to avoid multiple uses of the same coupon by the same user
    - category_rules: List[str] (optional) -> List of categories that the coupon is valid for
    - service_rules: List[str] (optional) -> List of service ids that the coupon is valid for
    - provider_rules: List[str] (optional) -> List of provider ids that the coupon is valid for
    - location_rule: (longitude and latitude) (optional) -> Location where the coupon is valid (center point)
    - max_distance: int (optional) -> Max distance from the location_rule where the coupon is valid
    - users_rules: List[str] (optional) -> List of user ids that the coupon is valid for
    """

    def __init__(self, test_client=None, test_db=None):
        self.client = test_client or get_mongo_client()
        if not self._check_connection():
            raise Exception("Failed to connect to MongoDB")
        if test_client:
            self.db = self.client[os.getenv('MONGO_TEST_DB')]
        else:
            self.db = self.client[test_db or os.getenv('MONGO_DB')]
        self.collection = self.db['payments']
        self._create_collection()
    
    def _check_connection(self):
        try:
            self.client.admin.command('ping')
        except Exception as e:
            logger.error(e)
            return False
        return True

    def _create_collection(self):
        self.collection.create_index([('uuid', ASCENDING)], unique=True)
        self.collection.create_index([('location', '2dsphere')])
    
    def insert(self, coupon_code: str, discount_percent: float, max_discount: Optional[float], expiration_date: int, category_rules: Optional[List[str]], service_rules: Optional[List[str]], provider_rules: Optional[List[str]], location_rule: Optional[dict], max_distance: Optional[int], users_rules: Optional[List[str]]) -> Optional[str]:
        try:
            self.collection.insert_one({
                'uuid': coupon_code,
                'discount_percent': discount_percent,
                'max_discount': max_discount,
                'expiration_date': expiration_date,
                'used_by': [],
                'category_rules': category_rules,
                'service_rules': service_rules,
                'provider_rules': provider_rules,
                'location_rule': {'type': 'Point', 'coordinates': [location_rule['longitude'], location_rule['latitude']]} if location_rule else None,
                'max_distance': max_distance,
                'users_rules': users_rules,
                'created_at': get_actual_time(),
                'updated_at': get_actual_time()
            })
            return coupon_code
        except DuplicateKeyError as e:
            logger.error(f"DuplicateKeyError: {e}")
            return None
        except OperationFailure as e:
            logger.error(f"OperationFailure: {e}")
            return None
        
    def get(self, coupon_code: str) -> Optional[Dict]:
        return self.collection.find_one({'uuid': coupon_code}) or None
    
    def delete(self, coupon_code: str) -> bool:
        result = self.collection.delete_one({'uuid': coupon_code})
        return result.deleted_count > 0
    
    def update(self, coupon_code: str, data: Dict) -> bool:
        data['updated_at'] = get_actual_time()
        try:
            result = self.collection.update_one({'uuid': coupon_code}, {'$set': data})
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating service with uuid '{uuid}': {e}")
            return False