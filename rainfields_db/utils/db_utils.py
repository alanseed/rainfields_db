# Contains get_db, get_config, write_config, get_parameters_df, get_central_wavelengths, 
# get_base_time

from typing import Optional, Dict
from urllib.parse import quote_plus
from pathlib import Path
from dotenv import load_dotenv 
import datetime
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo import MongoClient
from pymongo import ASCENDING,DESCENDING 
from pymongo import errors
import os
import logging 
import pandas as pd

def get_db(mongo_port: Optional[int] = None) -> Database:
    # === Load admin and user environment files ===
    admin_env = Path.home() / ".rainfields_admin.env"
    user_env = Path.home() / ".rainfields_user.env"
    load_dotenv(dotenv_path=admin_env, override=False)
    load_dotenv(dotenv_path=user_env, override=True)

    # === Connection settings ===
    mongo_host = os.getenv("MONGO_HOST", "localhost")
    mongo_port = mongo_port if mongo_port is not None else int(os.getenv("MONGO_PORT", 27017))
    target_db = os.getenv("DB_NAME", "rainfields_db")
    db_user = os.getenv("DB_USER")
    db_pwd = os.getenv("DB_PWD")

    if not db_user or not db_pwd:
        raise RuntimeError(f"Missing DB_USER or DB_PWD in environment. Check {user_env}.")

    logging.info(f"Using MONGO_PORT: {mongo_port} (overridden={mongo_port is not None})")

    connect_string = (
        f"mongodb://{quote_plus(db_user)}:{quote_plus(db_pwd)}"
        f"@{mongo_host}:{mongo_port}/{target_db}?authSource={target_db}"
    )
    logging.info(f"Connecting to MongoDB with: {connect_string}")
    client = MongoClient(connect_string)
    return client[target_db]

def get_config(db: Database, name: str) -> Dict:
    """_summary_
    Return the most recent configuration setting 
    Args:
        db (pymongo.MongoClient): Project database 

    Returns:
        Dict: Project configuration dictionary
    """

    config_coll = db["config"]
    record = config_coll.find_one({'config.name': name}, sort=[
                                  ('time', DESCENDING)])
    if record is None:
        logging.error(f"Could not find configuration for domain {name}")
        return {}

    config = record['config']
    return config

def write_config(config: dict):

    """Write the configuration to the config collection in the MongoDB database"""
    record = {
        "time": datetime.datetime.now(datetime.timezone.utc),
        "config": config
    }

    try:
        db = get_db()
        collection = db["config"]

        # Insert the record
        result = collection.insert_one(record)
        logging.info(
            f"Configuration inserted successfully. Document ID: {result.inserted_id}")

    except errors.ServerSelectionTimeoutError:
        logging.error(
            "Failed to connect to MongoDB. Check if MongoDB is running and the URI is correct.")
    except errors.PyMongoError as e:
        logging.error(f"MongoDB error: {e}")
