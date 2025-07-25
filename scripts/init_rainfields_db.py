from pymongo.database import Database 
from pymongo import ASCENDING 
from rainfields_db import get_db 

"""
Initialize MongoDB collections and indexes for rainfields system.

Collections created:
- rain.files / rain.chunks
- state.files / state.chunks
- stats
- params
- config
- domains

Indexes:
- Compound index on metadata for query efficiency
- Index on filename for GridFS deletion lookup
"""

# === Main ===
if __name__ == "__main__":
    db = get_db()

    for product in ["rain", "state"]:
        files_coll = f"{product}.files"
        chunks_coll = f"{product}.chunks"

        # Create empty collections (MongoDB creates on first insert, but we want indexes now)
        db[files_coll].insert_one({"temp": True})  # insert dummy
        db[chunks_coll].insert_one({"temp": True})

        # Create compound index on files
        db[files_coll].create_index([
            ("metadata.domain",ASCENDING),
            ("metadata.product", ASCENDING),
            ("metadata.valid_time", ASCENDING),
            ("metadata.base_time", ASCENDING),
            ("metadata.ensemble", ASCENDING)
        ], name="domain_product_valid_base_ensemble_idx")

        # Index for GridFS pre-deletion lookups
        db[files_coll].create_index([("filename", ASCENDING)], name="filename_idx")

        # Remove dummy record
        db[files_coll].delete_many({"temp": True})
        db[chunks_coll].delete_many({"temp": True})

        print(f"{files_coll} and {chunks_coll} initialized with index")

    # Create params collection
    params_coll = "params"
    db[params_coll].insert_one({"_test": True}) 
    db[params_coll].create_index([
        ("metadata.domain",ASCENDING),
        ("metadata.product", ASCENDING),
        ("metadata.valid_time", ASCENDING),
        ("metadata.base_time", ASCENDING),
        ("metadata.ensemble", ASCENDING)
    ], name="domain_product_valid_base_ensemble_idx")

    db[params_coll].delete_many({"_test": True})
    print(f"{params_coll} initialized")

    # Create stats collection
    stats_coll = "stats"
    db[stats_coll].insert_one({"_test": True}) 
    db[stats_coll].create_index([
        ("metadata.domain",ASCENDING),
        ("metadata.product", ASCENDING),
        ("metadata.valid_time", ASCENDING),
        ("metadata.base_time", ASCENDING),
        ("metadata.ensemble", ASCENDING)
    ], name="domain_product_valid_base_ensemble_idx")

    db[stats_coll].delete_many({"_test": True})
    print(f"{stats_coll} initialized")

    print("Setup complete.")

