#!/usr/bin/env python3

import os
import argparse
import secrets
import string
from pymongo import MongoClient
from urllib.parse import quote_plus
from pathlib import Path
from dotenv import load_dotenv

# === CONFIGURATION ===
load_dotenv(dotenv_path=Path.home() / ".rainfields_admin.env")

MONGO_HOST = str(os.getenv("MONGO_HOST"))
MONGO_PORT = os.getenv("MONGO_PORT")
AUTH_DB = os.getenv("AUTH_DB")
MONGO_ADMIN_USER = str(os.getenv("ADMIN_USER"))
MONGO_ADMIN_PASS = str(os.getenv("ADMIN_PWD"))
DEFAULT_DB = "rainfields_db"
DEFAULT_ROLE = "readWrite"
ENV_FILE = Path.home() / ".rainfields_user.env" 

# === FUNCTIONS ===

def generate_password(length=16):
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def create_user(username, db_name, role=DEFAULT_ROLE, password=None):
    if password is None:
        password = generate_password()

    mongo_uri = (
        f"mongodb://{quote_plus(MONGO_ADMIN_USER)}:{quote_plus(MONGO_ADMIN_PASS)}"
        f"@{MONGO_HOST}:{MONGO_PORT}/?authSource={AUTH_DB}"
    )
    client = MongoClient(mongo_uri)
    db = client[db_name]

    try:
        db.command("createUser", username, pwd=password, roles=[{"role": role, "db": db_name}])
        print(f"\n✅ User '{username}' created with role '{role}' on database '{db_name}'.\n")
        print("📌 Connection string:")
        print(f"  mongodb://{quote_plus(username)}:{quote_plus(password)}@{MONGO_HOST}:{MONGO_PORT}/{db_name}?authSource={db_name}\n")
        return password
    except Exception as e:
        print(f"❌ Failed to create user '{username}': {e}")
        return None

def write_password_to_env(username, password, db_name):
    with open(ENV_FILE, "a") as f:
        f.write(f"DB_NAME={db_name}\n")
        f.write(f"DB_USER={username}\n")
        f.write(f"DB_PWD={password}\n")
    print(f"🔐 Credentials saved to {ENV_FILE.absolute()} (append mode).")

# === ENTRY POINT ===

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a MongoDB user for a selected database.")
    parser.add_argument("username", help="Username to create")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"Target MongoDB database (default: {DEFAULT_DB})")
    parser.add_argument("--role", default=DEFAULT_ROLE, help="MongoDB role (default: readWrite)")
    parser.add_argument("--password", help="Optional password (otherwise generated)")

    args = parser.parse_args()

    password = create_user(args.username, args.db, args.role, args.password)
    if password:
        write_password_to_env(args.username, password, args.db)
