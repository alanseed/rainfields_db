from pymongo.database import Database 
from pymongo import ASCENDING 
import argparse 
from rainfields_db import get_db 

# === Functions ===
def setup_domain(db:Database, domain_name:str):
    print(f"Setting up domain: {domain_name}")

    for product in ["rain", "state"]:
        files_coll = f"{domain_name}.{product}.files"
        chunks_coll = f"{domain_name}.{product}.chunks"

        # Create empty collections (MongoDB creates on first insert, but we want indexes now)
        db[files_coll].insert_one({"temp": True})  # insert dummy
        db[chunks_coll].insert_one({"temp": True})

        # Create compound index on files
        db[files_coll].create_index([
            ("metadata.product", ASCENDING),
            ("metadata.valid_time", ASCENDING),
            ("metadata.base_time", ASCENDING),
            ("metadata.ensemble", ASCENDING)
        ], name="product_valid_base_ensemble_idx")

        # Index for GridFS pre-deletion lookups
        db[files_coll].create_index([("filename", ASCENDING)], name="filename_idx")

        # Remove dummy record
        db[files_coll].delete_many({"temp": True})
        db[chunks_coll].delete_many({"temp": True})

        print(f"{files_coll} and {chunks_coll} initialized with index")

    # Create a per-domain params collection
    params_coll = f"{domain_name}.params"
    db[params_coll].insert_one({"_test": True})
    db[params_coll].delete_many({"_test": True})
    print(f"{params_coll} initialized")

def setup_config(db:Database):
    config_coll = "config"
    db[config_coll].insert_one({"_test": True})
    db[config_coll].delete_many({"_test": True})
    print(f"{config_coll} initialized (shared)")

# === Main ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize STEPS MongoDB structure")
    parser.add_argument("domains", nargs="+", help="List of domain names to set up (e.g. AKL WLG CHC)")
    args = parser.parse_args()
    db = get_db()

    for domain in args.domains:
        setup_domain(db, domain)

    setup_config(db)
    print("Setup complete.")

