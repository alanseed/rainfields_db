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

def write_state(db: Database, cascade_dict: dict, oflow: np.ndarray,
                            file_name: str, field_metadata: dict):
    """
    Stores a pysteps cascade decomposition dictionary and optical flow into MongoDB's GridFSBucket.

    Parameters:
        db (Database): The MongoDB database object.
        cascade_dict (dict): The pysteps cascade decomposition dictionary.
        oflow (np.ndarray): The optical flow field.
        file_name (str): The unique name of the file to be stored.
        field_metadata (dict): Additional metadata related to the field.

    Returns:
        bson.ObjectId: The GridFS file ID.
    """
    assert cascade_dict["domain"] == "spatial", "Only 'spatial' domain is supported."
    bucket = GridFSBucket(db, bucket_name="state")

    # Delete existing file with same filename
    for old_file in db["state.files"].find({"filename": file_name}):
        bucket.delete(old_file["_id"])

    # Serialize cascade data to compressed binary
    buffer = BytesIO()
    np.savez_compressed(buffer, cascade_levels=cascade_dict["cascade_levels"], oflow=oflow)
    buffer.seek(0)

    # Build metadata
    metadata = {
        "domain": "spatial",
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

def get_state(db: Database, file_name: str):
    """
    Loads a pysteps cascade decomposition dictionary and optical flow from MongoDB's GridFSBucket.
    Returns empty structures if the file is not in the database (assumed dry).

    Parameters:
        db (Database): The MongoDB database object.
        file_name (str): The name of the file to retrieve.

    Returns:
        tuple: (cascade_dict, oflow, metadata)
    """
    bucket = GridFSBucket(db, bucket_name="state")
    mname = "state.files"

    # Look up metadata first
    result = db[mname].find_one({"filename": file_name})
    if result is None:
        logging.info(f"{file_name} not listed in {mname} collection — assumed dry.")
        return {}, np.array([]), {}

    metadata = result.get("metadata", {})

    try:
        with bucket.open_download_stream_by_name(file_name) as stream:
            if stream.metadata is None:
                raise ValueError(f"File '{file_name}' is missing required metadata.")

            buffer = BytesIO(stream.read())
    except Exception as e:
        logging.warning(f"Failed to download {file_name} from GridFS: {e}")
        return {}, np.array([]), metadata  # Return empty cascade and oflow, but retain metadata

    npzfile = np.load(buffer)

    cascade_dict = {
        "cascade_levels": npzfile["cascade_levels"],
        "domain": "spatial",
        "normalized": metadata.get("normalized"),
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

def get_rainfield(db: Database, filename: str) -> Tuple[xr.DataArray, dict]:
    """
    Read a rain field from GridFSBucket by filename and return the DataArray and metadata.
    If the file is not listed in metadata (e.g., dry period), return an empty DataArray and metadata.

    Args:
        db (Database): The MongoDB database connection.
        filename (str): Unique filename (as returned by make_nc_name).

    Returns:
        Tuple[xr.DataArray, dict]: Rain field and metadata dictionary.
    """
    bname = "rain"
    mname = "rain.files"
    result = db[mname].find_one({"filename": filename})
    
    if result is None:
        logging.info(f"{filename} not listed in {mname} collection — assumed dry.")
        return xr.DataArray(), {}

    metadata = result.get("metadata", {})

    # Only attempt GridFS read if metadata exists
    bucket = GridFSBucket(db, bucket_name=bname)
    buf = BytesIO()
    try:
        bucket.download_to_stream_by_name(filename, buf)
        buf.seek(0)
        rainfield, _ = read_netcdf_buffer(buf.read())
    except Exception as e:
        logging.error(f"Failed to download {filename} from GridFS: {e}")
        return xr.DataArray(), metadata  # Metadata exists, but file is missing

    return rainfield, metadata

def get_states_df(db: Database, query: dict,
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
        domain = metadata_dict.get("domain")
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
                "domain": "spatial",
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
            "domain":domain,
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

def get_rainfields_df(db: Database, query: dict) -> pd.DataFrame:
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
        logging.warning(f"No rain files found in 'rain.files' for query: {query}")
        return pd.DataFrame(columns=[
            "product", "valid_time", "base_time", "ensemble", "rainfield", "metadata"
        ])
    
    records = []

    for doc in results:
        rain_file = doc["filename"]

        metadata_dict = doc.get("metadata", {})
        domain = metadata_dict.get("domain")
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
            "domain":domain,
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
    domain: str,
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
        "domain": domain,
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