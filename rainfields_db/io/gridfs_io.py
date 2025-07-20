# Contains: 
# write_state, get_state, 
# write_rainfield, get_rainfield, 
# get_states_df, get_rainfields_df, 
# make_metadata

from pymongo.database import Database
from gridfs import GridFSBucket
from io import BytesIO
import xarray as xr
import numpy as np
import pandas as pd
import logging
import datetime
import copy
from pymongo import ASCENDING
from typing import Optional, Tuple
from rainfields_db import read_netcdf_buffer

def write_state(db: Database, name: str, cascade_dict: dict, oflow: np.ndarray,
                            file_name: str, field_metadata: dict):
    """
    Stores a pysteps cascade decomposition dictionary and optical flow into MongoDB's GridFSBucket.

    Parameters:
        db (Database): The MongoDB database object.
        name (str): Domain name used for the GridFS bucket (e.g., "AKL").
        cascade_dict (dict): The pysteps cascade decomposition dictionary.
        oflow (np.ndarray): The optical flow field.
        file_name (str): The unique name of the file to be stored.
        field_metadata (dict): Additional metadata related to the field.

    Returns:
        bson.ObjectId: The GridFS file ID.
    """
    assert cascade_dict["domain"] == "spatial", "Only 'spatial' domain is supported."
    bucket = GridFSBucket(db, bucket_name=f"{name}.state")

    # Delete existing file with same filename
    for old_file in db[f"{name}.state.files"].find({"filename": file_name}):
        bucket.delete(old_file["_id"])

    # Serialize cascade data to compressed binary
    buffer = BytesIO()
    np.savez_compressed(buffer, cascade_levels=cascade_dict["cascade_levels"], oflow=oflow)
    buffer.seek(0)

    # Build metadata
    metadata = {
        "domain": cascade_dict["domain"],
        "normalized": cascade_dict["normalized"],
        "transform": cascade_dict.get("transform"),
        "threshold": cascade_dict.get("threshold"),
        "zerovalue": cascade_dict.get("zerovalue")
    }
    metadata.update(field_metadata)

    if "means" in cascade_dict:
        metadata["means"] = cascade_dict["means"]
    if "stds" in cascade_dict:
        metadata["stds"] = cascade_dict["stds"]

    # Store file in GridFSBucket
    file_id = bucket.upload_from_stream(file_name, buffer, metadata=metadata)
    return file_id

def get_state(db: Database, name: str, file_name: str):
    """
    Loads a pysteps cascade decomposition dictionary and optical flow from MongoDB's GridFSBucket.

    Parameters:
        db (Database): The MongoDB database object.
        name (str): The domain name used to identify the collection.
        file_name (str): The name of the file to retrieve.

    Returns:
        tuple: (cascade_dict, oflow, metadata)
    """
    bucket = GridFSBucket(db, bucket_name=f"{name}.state")

    try:
        with bucket.open_download_stream_by_name(file_name) as stream:
            metadata = stream.metadata
            if metadata is None:
                raise ValueError(f"File '{file_name}' is missing required metadata.")

            buffer = BytesIO(stream.read())
    except Exception as e:
        raise ValueError(f"Error reading '{file_name}' from GridFS: {e}")

    npzfile = np.load(buffer)

    cascade_dict = {
        "cascade_levels": npzfile["cascade_levels"],
        "domain": metadata["domain"],
        "normalized": metadata["normalized"],
        "transform": metadata.get("transform"),
        "threshold": metadata.get("threshold"),
        "zerovalue": metadata.get("zerovalue"),
    }

    if "means" in metadata:
        cascade_dict["means"] = metadata["means"]
    if "stds" in metadata:
        cascade_dict["stds"] = metadata["stds"]

    oflow = npzfile["oflow"]
    return cascade_dict, oflow, metadata


def write_rainfield(db: Database, filename: str, nc_buf, metadata: dict) -> None:
    """
    Uploads a NetCDF rain field to MongoDB's GridFSBucket, replacing any existing file.

    Parameters:
        db (Database): The MongoDB database object.
        name (str): The domain name, used to derive the bucket name.
        filename (str): The unique filename for the rain field.
        nc_buf (BytesIO or memoryview): The NetCDF binary buffer.
        metadata (dict): Metadata to associate with the file.
    """
    bucket = GridFSBucket(db, bucket_name="rain")

    # Delete existing file with same filename if it exists
    for old_file in db["rain.files"].find({"filename": filename}):
        bucket.delete(old_file["_id"])

    # Upload new file
    nc_buf.seek(0)
    bucket.upload_from_stream(filename, nc_buf, metadata=metadata)


def get_rainfield(db: Database, name: str, filename: str) -> Tuple[xr.DataArray, dict]:
    """
    Read a rain field from GridFSBucket by filename and return the DataArray and metadata.

    Args:
        db (Database): The MongoDB database connection.
        name (str): Domain name (used to construct bucket name).
        filename (str): Unique filename (as returned by make_nc_name).

    Returns:
        Tuple[xr.DataArray, dict]: Rain field and metadata dictionary.
    """
    bname = "rain"
    bucket = GridFSBucket(db, bucket_name=bname)

    buf = BytesIO()
    try:
        bucket.download_to_stream_by_name(filename, buf)
    except Exception as e:
        logging.error(f"Could not download {filename} from bucket {bname}: {e}")
        raise

    buf.seek(0)
    rainfield, vtime = read_netcdf_buffer(buf.read())

    # Retrieve metadata
    mname = "rain.files"
    result = db[mname].find_one({"filename": filename})
    metadata = result.get("metadata", {}) if result else {}

    return rainfield, metadata

def get_states_df(db: Database, name: str, query: dict,
                   get_cascade: Optional[bool] = True,
                   get_optical_flow: Optional[bool] = True
                   ) -> pd.DataFrame:
    """
    Retrieve state fields (cascade and/or optical flow) from a GridFSBucket,
    returned as a pandas DataFrame with columns: product, valid_time, base_time, ensemble, cascade, optical_flow, metadata.
    """
    fs = GridFSBucket(db, bucket_name="state")
    meta_coll = db["state.files"]

    fields = {"_id": 0, "filename": 1, "metadata": 1}
    results_cursor = meta_coll.find(query, projection=fields).sort("filename", ASCENDING)
    results = list(results_cursor)

    if not results:
        logging.warning(f"No state files found for query: {query}")
        return pd.DataFrame(columns=[
            "product", "valid_time", "base_time", "ensemble",
            "cascade", "optical_flow", "metadata"
        ])

    records = []

    for doc in results:
        state_file = doc["filename"]
        metadata_dict = doc.get("metadata", {})

        product = metadata_dict.get("product")
        valid_time = metadata_dict.get("valid_time") 
        base_time = metadata_dict.get("base_time")
        ensemble = metadata_dict.get("ensemble")

        if valid_time is None:
            logging.warning(f"No valid_time in metadata for file {state_file}, skipping.")
            continue
        if valid_time.tzinfo is None:
            valid_time = valid_time.replace(tzinfo=datetime.timezone.utc)

        if base_time is not None and base_time.tzinfo is None:
            base_time = base_time.replace(tzinfo=datetime.timezone.utc)

        try:
            buffer = BytesIO()
            fs.download_to_stream_by_name(state_file, buffer)
            buffer.seek(0)             
            npzfile = np.load(buffer)
        except Exception as e:
            logging.warning(f"Could not load state file {state_file}: {e}")
            continue

        cascade_dict = None
        if get_cascade:
            cascade_dict = {
                "cascade_levels": npzfile["cascade_levels"],
                "domain": metadata_dict.get("domain"),
                "normalized": metadata_dict.get("normalized"),
                "transform": metadata_dict.get("transform"),
                "threshold": metadata_dict.get("threshold"),
                "zerovalue": metadata_dict.get("zerovalue"),
                "means": metadata_dict.get("means"),
                "stds": metadata_dict.get("stds"),
            }

        oflow = None
        if get_optical_flow:
            oflow = npzfile["oflow"]

        records.append({
            "name":name,
            "product":product,
            "valid_time": valid_time,
            "base_time": base_time,
            "ensemble": ensemble,
            "cascade": copy.deepcopy(cascade_dict) if cascade_dict is not None else None,
            "optical_flow": oflow.copy() if oflow is not None else None,
            "metadata": copy.deepcopy(metadata_dict)
        })
    df = pd.DataFrame(records)
    df["valid_time"] = df["valid_time"].astype("object")
    df["base_time"] = df["base_time"].astype("object")
    return df

def get_rainfields_df(db: Database, name: str, query: dict) -> pd.DataFrame:
    """
    Retrieve rainfields from a GridFSBucket,
    returned as a pandas DataFrame with columns: product, valid_time, base_time, ensemble, rainfield, metadata.
    valid_time and base_time are timezone-aware datetime.datetime objects or None.
    """
    fs = GridFSBucket(db, bucket_name="rain")
    meta_coll = db["rain.files"]

    fields = {"_id": 0, "filename": 1, "metadata": 1}
    results_cursor = meta_coll.find(query, projection=fields).sort("filename", ASCENDING)
    results = list(results_cursor)

    if not results:
        logging.warning(f"No rain files found in '{name}.rain.files' for query: {query}")
        return pd.DataFrame(columns=[
            "product", "valid_time", "base_time", "ensemble", "rainfield", "metadata"
        ])
    
    records = []

    for doc in results:
        rain_file = doc["filename"]

        metadata_dict = doc.get("metadata", {})
        product = metadata_dict.get("product")
        valid_time = metadata_dict.get("valid_time")
        base_time = metadata_dict.get("base_time")
        ensemble = metadata_dict.get("ensemble")

        if valid_time is None:
            logging.warning(f"No valid_time in metadata for file {rain_file}, skipping.")
            continue
        if valid_time.tzinfo is None or valid_time.tzinfo.utcoffset(valid_time) is None:
            valid_time = valid_time.replace(tzinfo=datetime.timezone.utc)

        if base_time is not None:
            if base_time.tzinfo is None or base_time.tzinfo.utcoffset(base_time) is None:
                base_time = base_time.replace(tzinfo=datetime.timezone.utc)

        try:
            buffer = BytesIO()
            fs.download_to_stream_by_name(rain_file, buffer)
            buffer.seek(0)
            rainfield, _ = read_netcdf_buffer(buffer.read())
        except Exception as e:
            logging.warning(f"Could not load rainfield from '{rain_file}': {e}")
            continue

        records.append({
            "name":name,
            "product": product,
            "valid_time": valid_time,
            "base_time": base_time,
            "ensemble": ensemble,
            "rainfield": rainfield,
            "metadata": metadata_dict
        })

    df = pd.DataFrame(records)
    df["valid_time"] = df["valid_time"].astype("object")
    df["base_time"] = df["base_time"].astype("object")

    return df

def make_metadata(
    rain: xr.DataArray,
    product: str,
    name: str,
    valid_time: datetime.datetime,
    base_time: Optional[datetime.datetime] = None,
    ensemble: Optional[int] = None
) -> dict:
    """
    Generate the metadata dictionary to be written to the database.

    Returns:
        dict: Metadata dictionary with statistics and timing.
    """
    if valid_time.tzinfo is None or valid_time.tzinfo.utcoffset(valid_time) is None:
        raise ValueError("valid_time must be timezone-aware (UTC)")

    if base_time is not None and (base_time.tzinfo is None or base_time.tzinfo.utcoffset(base_time) is None):
        raise ValueError("base_time must be timezone-aware (UTC) if provided")

    war = np.count_nonzero(rain >= 1) / rain.size

    metadata = {
        "domain": name,
        "product": product,
        "valid_time": valid_time,
        "base_time": base_time,
        "ensemble": ensemble,
        "mean": float(np.round(rain.mean(),3)),
        "wetted_area_ratio": float(np.round(war,3)),
        "std_dev": float(np.round(rain.std(),3)),
        "max": float(np.round(rain.max(),3)),
        "forecast_lead_time": (valid_time - base_time).total_seconds() if base_time else None
    }

    return metadata