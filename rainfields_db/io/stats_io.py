from pymongo.collection import Collection
from typing import List, Dict
from pymongo import ASCENDING

def get_stats_docs(query: Dict, coll: Collection) -> List[Dict]:
    """Retrieve field statistics documents from MongoDB."""
    return list(coll.find(query).sort("metadata.valid_time", ASCENDING))


def write_stats_docs(docs: List[Dict], coll: Collection):
    """Write a list of field statistics documents to MongoDB."""
    if docs:
        coll.insert_many(docs)