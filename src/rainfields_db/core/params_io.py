from pymongo.collection import Collection
from typing import List, Dict
from pymongo import ASCENDING

def get_param_docs(query: Dict, coll: Collection) -> List[Dict]:
    """Retrieve raw parameter documents from MongoDB."""
    return list(coll.find(query).sort("metadata.valid_time", ASCENDING))


def write_param_docs(docs: List[Dict], coll: Collection):
    """Write a list of parameter documents to MongoDB."""
    if docs:
        coll.insert_many(docs)