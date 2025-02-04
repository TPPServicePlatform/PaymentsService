import datetime
import os
import time
from typing import Optional, Union
from fastapi import HTTPException
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import logging as logger
import re
import geopy.distance

DAY = 24 * 60 * 60
HOUR = 60 * 60
MINUTE = 60
MILLISECOND = 1_000

REQUIRED_LOCATION_FIELDS = {"longitude", "latitude"}

def time_to_string(time_in_seconds: float) -> str:
    minutes = int(time_in_seconds // MINUTE)
    seconds = int(time_in_seconds % MINUTE)
    millis = int((time_in_seconds - int(time_in_seconds)) * MILLISECOND)
    return f"{minutes}m {seconds}s {millis}ms"

def get_mongo_client() -> MongoClient:
    uri = f"mongodb+srv://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}/?retryWrites=true&w=majority&appName={os.getenv('MONGO_APP_NAME')}"
    print(f"Connecting to MongoDB: {uri}")
    logger.getLogger('pymongo').setLevel(logger.WARNING)
    return MongoClient(uri, server_api=ServerApi('1'))

def get_actual_time() -> str:
    return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

def is_float(value):
    float_pattern = re.compile(r'^-?\d+(\.\d+)?$')
    return bool(float_pattern.match(value))

def validate_fields(data: dict, required_fields: set, valid_fields: set) -> None:
    extra_fields = set(data.keys()) - valid_fields
    missing_fields = required_fields - set(data.keys())
    if extra_fields:
        raise HTTPException(status_code=400, detail=f"Extra fields: {extra_fields}")
    if missing_fields:
        raise HTTPException(status_code=400, detail=f"Missing fields: {missing_fields}")

def validate_location(client_location, required_fields):
    if type(client_location) == str:
        if client_location.count(",") != 1:
            raise HTTPException(status_code=400, detail="Invalid location (must be in the format 'longitude,latitude')")
        client_location = client_location.split(",")
        client_location = {"longitude": client_location[0], "latitude": client_location[1]}
    elif type(client_location) == dict:
        if not all([field in client_location for field in required_fields]):
            missing_fields = required_fields - set(client_location.keys())
            raise HTTPException(status_code=400, detail=f"Missing location fields: {', '.join(missing_fields)}")
    else:
        raise HTTPException(status_code=400, detail="Invalid location (must be a string or a dictionary)")
    if not all([type(value) in [int, float] or is_float(value) for value in client_location.values()]):
        raise HTTPException(status_code=400, detail="Invalid location (each value must be a float)")
    client_location = {key: float(value) for key, value in client_location.items()}
    return client_location

def calculate_distance(location1: dict, location2: dict) -> float:
    coords1 = (location1['latitude'], location1['longitude'])
    coords2 = (location2['latitude'], location2['longitude'])
    return geopy.distance.distance(coords1, coords2).km

def get_timestamp_after_days(days: int) -> str:
    return datetime.datetime.fromtimestamp(time.time() + days * DAY).strftime('%Y-%m-%d %H:%M:%S')


def verify_coupon_rules(coupon, user_id, category, service_id, provider_id, client_location):
    validate = lambda item, rule: len(coupon.get(rule) or []) == 0 or item in coupon[rule]
    
    if user_id in coupon.get('used_by', {}):
        return False, "Coupon already used by this user"
    
    if get_actual_time() > coupon['expiration_date']:
        return False, "Coupon expired"
    
    if not validate(category, 'category_rules'):
        return False, "Category rule not satisfied"
    
    if not validate(service_id, 'service_rules'):
        return False, "Service rule not satisfied"
    
    if not validate(provider_id, 'provider_rules'):
        return False, "Provider rule not satisfied"
    
    if 'location_rule' in coupon and coupon['location_rule']:
        location = validate_location(client_location, REQUIRED_LOCATION_FIELDS)
        distance = calculate_distance(location, coupon['location_rule'])
        if distance > coupon['max_distance']:
            return False, "Location rule not satisfied"
        
    if not validate(user_id, 'users_rules'):
        return False, "User rule not satisfied"
    
    return True, ""