from typing import Optional, List, Dict
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError, OperationFailure
import logging as logger
import os
import sys
import uuid
from lib.utils import get_actual_time, get_mongo_client

HOUR = 60 * 60
MINUTE = 60
MILLISECOND = 1_000

# TODO: (General) -> Create tests for each method && add the required checks in each method


class Coupons:
    """
    Coupons class that stores data in a MongoDB collection.
    Fields:
    - id: str (unique) (coupon code)
    - discount_percent: float
    - max_discount: float (optional)
    - expiration_date: datetime
    - created_at: datetime
    - updated_at: datetime
    - used_by: Dict[str, str] -> List of user ids that used the coupon, the idea is to avoid multiple uses of the same coupon by the same user. It has the following structure: {'user_id': 'used_at'}
    - category_rules: List[str] (optional) -> List of categories that the coupon is valid for
    - service_rules: List[str] (optional) -> List of service ids that the coupon is valid for
    - provider_rules: List[str] (optional) -> List of provider ids that the coupon is valid for
    - location_rule: (longitude and latitude) (optional) -> Location where the coupon is valid (center point)
    - max_distance: int (optional) -> Max distance from the location_rule where the coupon is valid (kilometers)
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

    def insert(self,
               coupon_code: str,
               discount_percent: float,
               expiration_date: int,
               max_discount: Optional[float] = None,
               category_rules: Optional[List[str]] = None,
               service_rules: Optional[List[str]] = None,
               provider_rules: Optional[List[str]] = None,
               location_rule: Optional[dict] = None,
               max_distance: Optional[int] = None,
               users_rules: Optional[List[str]] = None
               ) -> bool:
        try:
            self.collection.insert_one({
                'uuid': coupon_code,
                'discount_percent': discount_percent,
                'max_discount': max_discount,
                'expiration_date': expiration_date,
                'used_by': {},
                'category_rules': category_rules,
                'service_rules': service_rules,
                'provider_rules': provider_rules,
                'location_rule': {'type': 'Point', 'coordinates': [location_rule['longitude'], location_rule['latitude']]} if location_rule else None,
                'max_distance': max_distance,
                'users_rules': users_rules,
                'created_at': get_actual_time(),
                'updated_at': get_actual_time()
            })
            return True
        except DuplicateKeyError as e:
            logger.error(f"DuplicateKeyError: {e}")
            return False
        except OperationFailure as e:
            logger.error(f"OperationFailure: {e}")
            return False

    def get(self, coupon_code: str) -> Optional[Dict]:
        return self.collection.find_one({'uuid': coupon_code}) or None

    def delete(self, coupon_code: str) -> bool:
        result = self.collection.delete_one({'uuid': coupon_code})
        return result.deleted_count > 0

    def update(self, coupon_code: str, data: Dict) -> bool:
        data['updated_at'] = get_actual_time()
        try:
            result = self.collection.update_one(
                {'uuid': coupon_code}, {'$set': data})
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating service with uuid '{uuid}': {e}")
            return False

    def obtain_available_coupons(self,
                                 user_id: str,
                                 client_location: dict,
                                 category: str,
                                 service_id: str,
                                 provider_id: str
                                 ) -> List[Dict]:
        pipeline = []

        # Filter by expiration date
        pipeline.append(
            {'$match': {'expiration_date': {'$gte': get_actual_time()}}})

        # Filter by category
        # If the coupon has no category rules, it is valid for all categories
        pipeline.append({'$match': {'$or': [{'category_rules': {'$exists': False}}, {
                        'category_rules': None}, {'category_rules': {'$in': [category]}}]}})

        # Filter by service
        # If the coupon has no service rules, it is valid for all services
        pipeline.append({'$match': {'$or': [{'service_rules': {'$exists': False}}, {
                        'service_rules': None}, {'service_rules': {'$in': [service_id]}}]}})

        # Filter by provider
        # If the coupon has no provider rules, it is valid for all providers
        pipeline.append({'$match': {'$or': [{'provider_rules': {'$exists': False}}, {
                        'provider_rules': None}, {'provider_rules': {'$in': [provider_id]}}]}})

        # Filter by location and max distance
        # If the coupon has no location rule, it is valid for all locations
        if not os.environ.get('MONGOMOCK'):
            geo_near_stage = {
                '$geoNear': {
                    'near': {
                        'type': 'Point',
                        'coordinates': [client_location['longitude'], client_location['latitude']]
                    },
                    'distanceField': 'distance',
                    'spherical': True
                }
            }
            pipeline.append(geo_near_stage)

            match_stage = {
                '$match': {
                    # Convert kilometers to meters
                    '$or': [{'max_distance': {'$exists': False}}, {'max_distance': None}, {'$expr': {'$lte': ['$distance', {'$multiply': ['$max_distance', 1000]}]}}]
                }
            }
            pipeline.append(match_stage)

        # Filter by user
        # If the coupon has no user rules, it is valid for all users
        pipeline.append({'$match': {'$or': [{'users_rules': {'$exists': False}}, {
                        'users_rules': None}, {'users_rules': {'$in': [user_id]}}]}})

        # Filter by used_by (avoid multiple uses of the same coupon by the same user)
        # used_by is a dictionary with the following structure: {'user_id': 'used_at'}
        pipeline.append({'$match': {'$or': [{'used_by': {'$exists': False}}, {
                        'used_by': {}}, {'used_by.' + user_id: {'$exists': False}}]}})

        # Project only the necessary fields
        pipeline.append({'$project': {'_id': 0, 'uuid': 1,
                        'discount_percent': 1, 'max_discount': 1, 'expiration_date': 1}})

        return list(self.collection.aggregate(pipeline))

    def get_refund_coupons(self, user_id: str) -> List[Dict]:
        return list(self.collection.find({
            'uuid': {'$regex': f'^REFUND_{user_id}_'},
            f'used_by.{user_id}': {'$exists': False}
        }, {
            '_id': 0,
            'uuid': 1,
            'coupon_code': 1,
            'max_discount': 1,
            'expiration_date': 1,
            'discount_percent': 1,
        }))

    def get_all_coupons(self) -> List[Dict]:
        return list(self.collection.find({}, {'_id': 0}))

    def obtain_user_coupons(self, user_id: str, client_location: dict) -> List[Dict]:
        pipeline = []

        if not os.environ.get('MONGOMOCK'):
            geo_near_stage = {
                '$geoNear': {
                    'near': {
                        'type': 'Point',
                        'coordinates': [client_location['longitude'], client_location['latitude']]
                    },
                    'distanceField': 'distance',
                    'spherical': True
                }
            }
            pipeline.append(geo_near_stage)

            match_distance_stage = {
                '$match': {
                    '$or': [
                        {'max_distance': {'$exists': False}},
                        {'max_distance': None},
                        {'$expr': {
                            '$lte': ['$distance', {'$multiply': ['$max_distance', 1000]}]
                        }}
                    ]
                }
            }
            pipeline.append(match_distance_stage)

        # ✅ El resto de los filtros sí pueden ir luego
        # Filtro por expiración
        pipeline.append({
            '$match': {
                'expiration_date': {'$gte': get_actual_time()}
            }
        })

        # Filtro por usuarios
        pipeline.append({
            '$match': {
                '$or': [
                    {'users_rules': {'$exists': False}},
                    {'users_rules': None},
                    {'users_rules': {'$in': [user_id]}}
                ]
            }
        })

        # Filtro por cupones usados
        pipeline.append({
            '$match': {
                '$or': [
                    {'used_by': {'$exists': False}},
                    {'used_by': {}},
                    {f'used_by.{user_id}': {'$exists': False}}
                ]
            }
        })

        # Proyección final
        pipeline.append({
            '$project': {
                '_id': 0,
                'uuid': 1,
                'discount_percent': 1,
                'max_discount': 1,
                'expiration_date': 1
            }
        })

        return list(self.collection.aggregate(pipeline))

    def mark_coupon_as_used(self, coupon_code: str, user_id: str) -> bool:
        try:
            result = self.collection.update_one(
                {'uuid': coupon_code},
                {'$set': {f'used_by.{user_id}': get_actual_time()}}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error marking coupon {coupon_code} as used: {e}")
            return False

    def add_user_to_coupon(self, coupon_code: str, user_id: str) -> bool:
        coupon = self.get(coupon_code)
        if not coupon:
            return False
        used_by = {**coupon.get('used_by', {}), user_id: get_actual_time()}
        try:
            result = self.collection.update_one(
                {'uuid': coupon_code}, {'$set': {'used_by': used_by}})
            return result.modified_count > 0
        except Exception as e:
            logger.error(
                f"Error adding user '{user_id}' to coupon '{coupon_code}': {e}")
            return False

    def add_item_to_rule(self, coupon_code: str, rule: str, item: str) -> bool:
        # verify rule
        coupon = self.get(coupon_code)
        if not coupon or rule not in coupon or len(coupon[rule] or []) == 0:
            return False
        try:
            result = self.collection.update_one(
                {'uuid': coupon_code}, {'$push': {rule: item}})
            return result.modified_count > 0
        except Exception as e:
            logger.error(
                f"Error adding item '{item}' to rule '{rule}' of coupon '{coupon_code}': {e}")
            return False
