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
from models.steps_params import StochasticRainParameters 
from models.cascade_utils import get_cascade_wavelengths 

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


def get_parameters_df(query: Dict, param_coll:Collection) -> pd.DataFrame:
    """
    Retrieve STEPS parameters from the database and return a DataFrame
    indexed by (valid_time, base_time, ensemble).

    Args:
        query (dict): MongoDB query dictionary.
        param_coll (pymongo.collection.Collection): MongoDB collection.

    Returns:
        pd.DataFrame: Indexed by (valid_time, base_time, ensemble), with a 'param' column.
    """
    records = []

    for doc in param_coll.find(query).sort("metadata.valid_time", ASCENDING):
        try:
            metadata = doc.get("metadata", {}) 
            if metadata is None:
                continue 

            # if doc["cascade"]["lag1"] is None or doc["cascade"]["lag2"] is None:
            #     continue

            valid_time = metadata.get("valid_time")
            if valid_time is not None and valid_time.tzinfo is None:
                valid_time = valid_time.replace(tzinfo=datetime.timezone.utc)

            base_time = metadata.get("base_time")
            if base_time is not None and base_time.tzinfo is None:
                base_time = base_time.replace(tzinfo=datetime.timezone.utc)

            ensemble = metadata.get("ensemble") if metadata.get("ensemble") is not None else None
            param = StochasticRainParameters.from_dict(doc)

            param.calc_corl()
            records.append({
                "valid_time": valid_time,
                "base_time": base_time,
                "ensemble": ensemble,
                "param": param
            })
        except Exception as e:
            print(f"Warning: could not parse parameter for {metadata.get('valid_time')}: {e}")  # type: ignore

    if not records:
        return pd.DataFrame({
            "valid_time": pd.Series(dtype='object'),
            "base_time": pd.Series(dtype='object'),
            "ensemble": pd.Series(dtype='object'),
            "param": pd.Series(dtype='object')  # holds StochasticRainParameters
        })

    df = pd.DataFrame(records)
    df["valid_time"] = df["valid_time"].astype("object")
    df["base_time"] = df["base_time"].astype("object")
    return df

def get_central_wavelengths(db:Database, name:str) -> float:
    config = get_config(db, name)
    n_levels = config["pysteps"].get("n_cascade_levels")
    domain = config["domain"]
    n_rows = domain.get("n_rows")
    n_cols = domain.get("n_cols")
    p_size = domain.get("p_size")
    p_size_km = p_size / 1000.0
    domain_size_km = max(n_rows, n_cols) * p_size_km

    # Get central wavelengths
    wavelengths_km = get_cascade_wavelengths(
        n_levels, domain_size_km, p_size_km)
    return wavelengths_km

def get_base_time(valid_time:datetime.datetime, product:str, name:str, db:Database) -> Optional[datetime.datetime]:
    # Get the base_time for the nwp run nearest to the valid_time in UTC zone
    # Assume spin-up of 3 hours
    start_base_time = valid_time - datetime.timedelta(hours=27)
    end_base_time = valid_time - datetime.timedelta(hours=3)
    base_time_query = {
        "metadata.product": product,
        "metadata.base_time": {"$gte": start_base_time, "$lte": end_base_time}
    }
    col_name = f"{name}.rain.files"
    nwp_base_times = db[col_name].distinct(
        "metadata.base_time", base_time_query)

    if not nwp_base_times:
        logging.warning(
            f"Failed to find {product} data for {valid_time}")
        return None 

    nwp_base_times.sort(reverse=True)
    base_time = nwp_base_times[0]

    if base_time.tzinfo is None:
        base_time = base_time.replace(tzinfo=datetime.timezone.utc)

    return base_time
