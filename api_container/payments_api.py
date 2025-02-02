import operator
import re
from typing import Optional, Tuple
from coupons_nosql import Coupons
import mongomock
import logging as logger
import time
from fastapi import FastAPI, File, UploadFile, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'lib')))
from lib.utils import time_to_string, validate_fields, validate_location, get_actual_time, calculate_distance

time_start = time.time()

logger.basicConfig(format='%(levelname)s: %(asctime)s - %(message)s',
                   stream=sys.stdout, level=logger.INFO)
logger.info("Starting the app")
load_dotenv()

DEBUG_MODE = os.getenv("DEBUG_MODE").title() == "True"
if DEBUG_MODE:
    logger.getLogger().setLevel(logger.DEBUG)
logger.info("DEBUG_MODE: " + str(DEBUG_MODE))

app = FastAPI(
    title="Payments API",
    description="API for payments management",
    version="1.0.0",
    root_path=os.getenv("ROOT_PATH")
)

origins = [
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if os.getenv('TESTING'):
    client = mongomock.MongoClient()
    coupons_manager = Coupons(test_client=client)
else:
    coupons_manager = Coupons()

# CONSTANTS HERE #
REQUIRED_LOCATION_FIELDS = {"longitude", "latitude"}

REQUIRED_COUPON_CREATE_FIELDS = {'coupon_code', 'discount_percent', 'expiration_date'}
VALID_COUPON_RULES = {'category_rules', 'service_rules', 'provider_rules', 'location_rule', 'max_distance', 'users_rules'}
VALID_COUPON_CREATE_FIELDS = {'max_discount'} | VALID_COUPON_RULES | REQUIRED_COUPON_CREATE_FIELDS

starting_duration = time_to_string(time.time() - time_start)
logger.info(f"Payments API started in {starting_duration}")

# TODO: (General) -> Create tests for each endpoint && add the required checks in each endpoint


@app.post("/coupons/create")
def create_coupon(body: dict):
    validate_fields(body, REQUIRED_COUPON_CREATE_FIELDS, VALID_COUPON_CREATE_FIELDS)
    
    # At least one ruled is needed
    if not any([field in body for field in VALID_COUPON_RULES]):
        raise HTTPException(status_code=400, detail="At least one rule is needed")
    if bool('location_rule' in body) != bool('max_distance' in body):
        raise HTTPException(status_code=400, detail="Both location_rule and max_distance are needed, or none of them")
    if 'location_rule' in body:
        location = validate_location(body['location_rule'], REQUIRED_LOCATION_FIELDS)
    if body['discount_percent'] <= 0 or body['discount_percent'] > 100:
        raise HTTPException(status_code=400, detail="Invalid discount percent")

    if coupons_manager.get(body['coupon_code']):
        raise HTTPException(status_code=400, detail="Coupon code already exists")
    
    if not coupons_manager.insert(
        coupon_code=body.get('coupon_code'),
        discount_percent=body.get('discount_percent'),
        max_discount=body.get('max_discount'),
        expiration_date=body.get('expiration_date'),
        category_rules=body.get('category_rules'),
        service_rules=body.get('service_rules'),
        provider_rules=body.get('provider_rules'),
        location_rule=location if 'location_rule' in body else None,
        max_distance=body.get('max_distance'),
        users_rules=body.get('users_rules')
    ):
        raise HTTPException(status_code=500, detail="Failed to create the coupon")
    
    return {"status": "ok"}

@app.delete("/coupons/delete/{coupon_code}")
def delete_coupon(coupon_code: str):
    if not coupons_manager.get(coupon_code):
        raise HTTPException(status_code=404, detail="Coupon not found")
    
    if not coupons_manager.delete(coupon_code):
        raise HTTPException(status_code=500, detail="Failed to delete the coupon")
    
    return {"status": "ok"}

@app.get("/coupons")
def obtain_available_coupons(
    user_id: str = Query(...),
    client_location: str = Query(...),
    category: str = Query(...),
    service_id: str = Query(...),
    provider_id: str = Query(...)
):
    location = validate_location(client_location, REQUIRED_LOCATION_FIELDS)
    available_coupons = coupons_manager.obtain_available_coupons(
        user_id=user_id,
        client_location=location,
        category=category,
        service_id=service_id,
        provider_id=provider_id
    )
    
    return {"status": "ok", "coupons": available_coupons}

@app.put("/coupons/activate/{coupon_code}/{user_id}")
def activate_coupon(coupon_code: str, user_id: str, body: dict):
    needed = {'client_location', 'category', 'service_id', 'provider_id'}
    if not all([field in body for field in needed]):
        missing_fields = needed - set(body.keys())
        raise HTTPException(status_code=400, detail=f"Missing fields: {', '.join(missing_fields)}")
    
    coupon = coupons_manager.get(coupon_code)
    if not coupon:
        raise HTTPException(status_code=404, detail="Coupon not found")
    
    success, message = _verify_coupon_rules(coupon, user_id, body['category'], body['service_id'], body['provider_id'], body['client_location'])
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    
    if not coupons_manager.add_user_to_coupon(coupon_code, user_id):
        raise HTTPException(status_code=500, detail="Failed to activate the coupon")
    
    return {"status": "ok", "discount_percent": coupon['discount_percent'], "max_discount": coupon['max_discount']}

def _verify_coupon_rules(coupon, user_id, category, service_id, provider_id, client_location):
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
    

