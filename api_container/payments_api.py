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
from lib.utils import time_to_string

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

starting_duration = time_to_string(time.time() - time_start)
logger.info(f"Payments API started in {starting_duration}")

# TODO: (General) -> Create tests for each endpoint && add the required checks in each endpoint
