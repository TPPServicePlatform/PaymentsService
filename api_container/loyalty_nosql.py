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
from lib.utils import get_actual_time, get_mongo_client, get_time_past_days

HOUR = 60 * 60
MINUTE = 60
MILLISECOND = 1_000

EXPIRATION_TIME = 2 * 365 # 2 years

# TODO: (General) -> Create tests for each method && add the required checks in each method
class Loyalty:
    """
    Loyalty class that stores data in a MongoDB collection.
    Fields:
    - id: str (unique) (user id)
    - points: List[Tuple[str, str]] -> List of tuples with the following structure: (expiration timestamp, points)
    - created_at: datetime
    - updated_at: datetime
    - history: List[Dict[str, str]] -> List of transactions that the user made. It has the following keys: {'points', 'timestamp', 'description'}
    """

    def __init__(self, test_client=None, test_db=None):
        self.client = test_client or get_mongo_client()
        if not self._check_connection():
            raise Exception("Failed to connect to MongoDB")
        if test_client:
            self.db = self.client[os.getenv('MONGO_TEST_DB')]
        else:
            self.db = self.client[test_db or os.getenv('MONGO_DB')]
        self.collection = self.db['loyalty']
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
    
    def _create_user_doc(self, user_id: str) -> bool:
        try:
            self.collection.insert_one({
                'uuid': user_id,
                'points': [],
                'created_at': get_actual_time(),
                'updated_at': get_actual_time(),
                'history': []
            })
            return True
        except DuplicateKeyError as e:
            logger.error(f"DuplicateKeyError: {e}")
            return False
        except OperationFailure as e:
            logger.error(f"OperationFailure: {e}")
            return False
    
    def add_transaction(self, user_id: str, points: int, description: str) -> bool:
        if points == 0:
            return False
        user = self.collection.find_one({'uuid': user_id})
        if not user:
            if not self._create_user_doc(user_id):
                return False
            user = self.collection.find_one({'uuid': user_id})
        
        success = True
        user['points'] = [(expiration_date, points) for expiration_date, points in user['points'] if expiration_date > get_actual_time()]
        user['updated_at'] = get_actual_time()

        if points > 0:
            user['points'].append((get_time_past_days(EXPIRATION_TIME), points))
        elif sum([points for _, points in user['points']]) >= abs(points):
            to_delete = abs(points)
            sorted_points = sorted(user['points'], key=lambda x: x[0])
            for i, (expiration_date, points) in enumerate(sorted_points):
                if points >= to_delete:
                    sorted_points[i] = (expiration_date, points - to_delete)
                    break
                to_delete -= points
                sorted_points[i] = (expiration_date, 0)
            user['points'] = [(expiration_date, points) for expiration_date, points in sorted_points if points > 0]
        else:
            success = False

        if success:
            user['history'].append({'points': points, 'timestamp': get_actual_time(), 'description': description})

        try:
            self.collection.update_one({'uuid': user_id}, {'$set': user})
            return success
        except Exception as e:
            logger.error(f"Error updating user with uuid '{user_id}': {e}")
            return False
    
    def get_total_points(self, user_id: str) -> int:
        user = self.collection.find_one({'uuid': user_id})
        if not user:
            return 0
        return sum([points for expiration_date, points in user['points'] if expiration_date > get_actual_time()])
    
    def get_history(self, user_id: str) -> List[Dict]:
        user = self.collection.find_one({'uuid': user_id})
        if not user:
            return []
        return sorted(user['history'], key=lambda x: x['timestamp'], reverse=True)
    
    def get_expiring_points(self, user_id: str) -> List[Dict]:
        user = self.collection.find_one({'uuid': user_id})
        if not user:
            return []
        return sorted([{'points': points, 'expiration_date': expiration_date} for expiration_date, points in user['points'] if expiration_date > get_actual_time()], key=lambda x: x['expiration_date'])
    

        