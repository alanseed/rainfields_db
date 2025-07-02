from typing import Optional
from urllib.parse import quote_plus
from pathlib import Path
from dotenv import load_dotenv
from pymongo.database import Database
from pymongo import MongoClient
import os
import logging

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
