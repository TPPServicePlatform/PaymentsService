import operator
import re
from typing import Optional, Tuple
from mobile_token_nosql import MobileToken, send_notification
from coupons_nosql import Coupons
from loyalty_nosql import Loyalty
import mongomock
import logging as logger
import time
from fastapi import FastAPI, File, UploadFile, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import sys
import os
from lib.utils import sentry_init, time_to_string, validate_fields, validate_location, verify_coupon_rules, get_timestamp_after_days

time_start = time.time()

logger.basicConfig(format='%(levelname)s: %(asctime)s - %(message)s',
                   stream=sys.stdout, level=logger.INFO)
logger.info("Starting the app")
load_dotenv()

DEBUG_MODE = os.getenv("DEBUG_MODE").title() == "True"
if DEBUG_MODE:
    logger.getLogger().setLevel(logger.DEBUG)
logger.info("DEBUG_MODE: " + str(DEBUG_MODE))

sentry_init()

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
    loyalty_manager = Loyalty(test_client=client)
    mobile_token_manager = MobileToken(test_client=client)
else:
    coupons_manager = Coupons()
    loyalty_manager = Loyalty()
    mobile_token_manager = MobileToken()

REQUIRED_LOCATION_FIELDS = {"longitude", "latitude"}

REQUIRED_COUPON_CREATE_FIELDS = {'coupon_code', 'discount_percent', 'expiration_date'}
VALID_COUPON_RULES = {'category_rules', 'service_rules', 'provider_rules', 'location_rule', 'max_distance', 'users_rules'}
VALID_COUPON_CREATE_FIELDS = {'max_discount'} | VALID_COUPON_RULES | REQUIRED_COUPON_CREATE_FIELDS
REQUIRED_REFUND_FIELDS = {'user_id', 'amount'}

REQUIRED_TRANSACTION_FIELDS = {'points', 'description'}

POINTS_PER_CASH = lambda cash: cash / 10
CASH_COUPON_POINTS_NEEDED = lambda cash_discount: cash_discount * 10
DISCOUNT_COUPON_POINTS_NEEDED = lambda discount: discount * 100

YEAR = 365 # Days

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

@app.post("/couupons/new_refund")
def create_refund_coupon(body: dict):
    validate_fields(body, REQUIRED_REFUND_FIELDS, REQUIRED_REFUND_FIELDS)
    if body['amount'] <= 0:
        raise HTTPException(status_code=400, detail="Amount must be positive")
    
    code = f"REFUND_{body['user_id']}_{time.time()}"
    if not coupons_manager.insert(
        coupon_code=code,
        discount_percent=100,
        max_discount=body['amount'],
        expiration_date=get_timestamp_after_days(100*YEAR),
        users_rules=[body['user_id']]
    ):
        raise HTTPException(status_code=500, detail="Failed to create the coupon")
    
    send_notification(mobile_token_manager, body['user_id'], "Refund coupon", f"Refund coupon of {body['amount']} created")
    return {"status": "ok", "coupon_code": code}

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
    
    success, message = verify_coupon_rules(coupon, user_id, body['category'], body['service_id'], body['provider_id'], body['client_location'])
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    
    if not coupons_manager.add_user_to_coupon(coupon_code, user_id):
        raise HTTPException(status_code=500, detail="Failed to activate the coupon")
    
    return {"status": "ok", "discount_percent": coupon['discount_percent'], "max_discount": coupon['max_discount']}
    
@app.put("/loyalty/sum_points/{user_id}")
def add_loyalty_transaction(user_id: str, body: dict):
    validate_fields(body, REQUIRED_TRANSACTION_FIELDS, REQUIRED_TRANSACTION_FIELDS)
    if body['points'] <= 0:
        raise HTTPException(status_code=400, detail="Points must be positive")
    
    if not loyalty_manager.add_transaction(user_id, body['points'], body['description']):
        raise HTTPException(status_code=500, detail="Failed to add the transaction")
    
    return {"status": "ok"}

@app.put("/loyalty/use_points/cash_coupon/{user_id}")
def buy_cash_coupon(user_id: str, body: dict):
    validate_fields(body, {"CASH_DISCOUNT"}, {"CASH_DISCOUNT"})
    if body['CASH_DISCOUNT'] <= 0:
        raise HTTPException(status_code=400, detail="Discount must be positive")
    
    points_needed = CASH_COUPON_POINTS_NEEDED(body['CASH_DISCOUNT'])
    total_points = loyalty_manager.get_total_points(user_id)
    if not total_points or total_points < points_needed:
        raise HTTPException(status_code=400, detail="Not enough points")
    
    coupon_code=f"CASH_{user_id}_{body['CASH_DISCOUNT']}_{time.time()}" # Unique coupon code
    if not coupons_manager.insert(
        coupon_code=coupon_code,
        discount_percent=100,
        max_discount=body['CASH_DISCOUNT'],
        expiration_date=get_timestamp_after_days(YEAR),
        users_rules=[user_id]
    ):
        raise HTTPException(status_code=500, detail="Failed to create the coupon")
    
    if not loyalty_manager.add_transaction(user_id, -points_needed, f"Bought cash coupon of {body['CASH_DISCOUNT']} ({coupon_code})"):
        coupons_manager.delete(coupon_code)
        raise HTTPException(status_code=500, detail="Failed to use the points")
    
    return {"status": "ok", "coupon_code": coupon_code}
    
@app.put("/loyalty/use_points/discount_coupon/{user_id}")
def buy_discount_coupon(user_id: str, body: dict):
    validate_fields(body, {"DISCOUNT"}, {"DISCOUNT"})
    if body['DISCOUNT'] <= 0 or body['DISCOUNT'] > 100:
        raise HTTPException(status_code=400, detail="Invalid discount")
    
    points_needed = DISCOUNT_COUPON_POINTS_NEEDED(body['DISCOUNT'])
    total_points = loyalty_manager.get_total_points(user_id)
    if not total_points or total_points < points_needed:
        raise HTTPException(status_code=400, detail="Not enough points")
    
    coupon_code=f"DISCOUNT_{user_id}_{body['DISCOUNT']}perc_{time.time()}" # Unique coupon code
    if not coupons_manager.insert(
        coupon_code=coupon_code,
        discount_percent=body['DISCOUNT'],
        expiration_date=get_timestamp_after_days(YEAR),
        users_rules=[user_id]
    ):
        raise HTTPException(status_code=500, detail="Failed to create the coupon")
    
    if not loyalty_manager.add_transaction(user_id, -points_needed, f"Bought discount coupon of {body['DISCOUNT']}% ({coupon_code})"):
        coupons_manager.delete(coupon_code)
        raise HTTPException(status_code=500, detail="Failed to use the points")
    
    return {"status": "ok", "coupon_code": coupon_code}

@app.get("/loyalty/points/{user_id}")
def obtain_user_points(user_id: str):
    total_points = loyalty_manager.get_total_points(user_id)
    if total_points == None:
        raise HTTPException(status_code=404, detail="User does not have loyalty points yet")
    expiring_dates = loyalty_manager.get_expiring_points(user_id)
    return {"status": "ok", "total_points": total_points, "expiring_dates": expiring_dates}

@app.get("/loyalty/history/{user_id}")
def obtain_user_history(user_id: str):
    history = loyalty_manager.get_history(user_id)
    if history == None:
        raise HTTPException(status_code=404, detail="User does not have loyalty points yet")
    return {"status": "ok", "history": history}
