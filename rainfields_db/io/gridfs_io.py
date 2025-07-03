# Contains: store_cascade_to_gridfs, load_cascade_from_gridfs, load_rain_field, get_rain_fields, get_states
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
from typing import Optional
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

def read_state(db: Database, name: str, file_name: str):
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


def write_rainfield(db: Database, name: str, filename: str, nc_buf, metadata: dict) -> None:
    """
    Uploads a NetCDF rain field to MongoDB's GridFSBucket, replacing any existing file.

    Parameters:
        db (Database): The MongoDB database object.
        name (str): The domain name, used to derive the bucket name.
        filename (str): The unique filename for the rain field.
        nc_buf (BytesIO or memoryview): The NetCDF binary buffer.
        metadata (dict): Metadata to associate with the file.
    """
    bucket = GridFSBucket(db, bucket_name=f"{name}.rain")

    # Delete existing file with same filename if it exists
    for old_file in db[f"{name}.rain.files"].find({"filename": filename}):
        bucket.delete(old_file["_id"])

    # Upload new file
    nc_buf.seek(0)
    bucket.upload_from_stream(filename, nc_buf, metadata=metadata)


def read_rainfield(db:Database, name:str, file_id:str):
    """_summary_
    Read rain field from the GridFSBucket 
    Args:
        db (Database): database 
        name (str): Domain name
        file_id (str): _id of the file 

    Returns:
        _type_: _description_
    """

    bname = f"{name}.rain"
    bucket = GridFSBucket(db, bucket_name=bname)
    
    buf = BytesIO()
    bucket.download_to_stream(file_id, buf)
    buf.seek(0)
    rainfield,vtime = read_netcdf_buffer(buf.read())
    
    # Retrieve metadata
    mname = f"{name}.rain.files"
    result = db[mname].find_one({"_id": file_id})
    metadata = result.get("metadata", {}) if result is not None else None 
    
    return rainfield, metadata


def read_states_df(db: Database, name: str, query: dict,
                   get_cascade: Optional[bool] = True,
                   get_optical_flow: Optional[bool] = True
                   ) -> pd.DataFrame:
    """
    Retrieve state fields (cascade and/or optical flow) from a GridFSBucket,
    returned as a pandas DataFrame with columns: product, valid_time, base_time, ensemble, cascade, optical_flow, metadata.
    """
    fs = GridFSBucket(db, bucket_name=f"{name}.state")
    meta_coll = db[f"{name}.state.files"]

    fields = {"_id": 0, "filename": 1, "metadata": 1}
    results_cursor = meta_coll.find(query, projection=fields).sort("filename", ASCENDING)
    results = list(results_cursor)

    if not results:
        logging.warning(f"No state files found in '{name}.state.files' for query: {query}")
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
        base_time = metadata_dict.get("base_time", "NA")
        ensemble = metadata_dict.get("ensemble", "NA")

        if valid_time is None:
            logging.warning(f"No valid_time in metadata for file {state_file}, skipping.")
            continue
        if valid_time.tzinfo is None:
            valid_time = valid_time.replace(tzinfo=datetime.timezone.utc)

        if base_time is not None and base_time != "NA" and base_time.tzinfo is None:
            base_time = base_time.replace(tzinfo=datetime.timezone.utc)

        if base_time is None:
            base_time = "NA"
        if ensemble is None:
            ensemble = "NA"

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

def read_rainfields_df(db: Database, name: str, query: dict) -> pd.DataFrame:
    """
    Retrieve rainfields from a GridFSBucket,
    returned as a pandas DataFrame with columns: product, valid_time, base_time, ensemble, rainfield, metadata.
    where valid_time and base_time are datetime.datetime objects with UTC timezone.
    """
    fs = GridFSBucket(db, bucket_name=f"{name}.rain")
    meta_coll = db[f"{name}.rain.files"]

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

        if "metadata" not in doc:
            logging.warning(f"No metadata in document for file {rain_file}, skipping.")
            continue
        metadata_dict = doc["metadata"]

        product = metadata_dict.get("product")
        valid_time = metadata_dict.get("valid_time")
        base_time = metadata_dict.get("base_time", "NA")
        ensemble = metadata_dict.get("ensemble", "NA")

        if valid_time is None:
            logging.warning(f"No valid_time in metadata for file {rain_file}, skipping.")
            continue
        if valid_time.tzinfo is None or valid_time.tzinfo.utcoffset(valid_time) is None:
            valid_time = valid_time.replace(tzinfo=datetime.timezone.utc)

        if base_time is not None and base_time != "NA":
            if base_time.tzinfo is None or base_time.tzinfo.utcoffset(base_time) is None:
                base_time = base_time.replace(tzinfo=datetime.timezone.utc)
        else:
            base_time = "NA"

        if ensemble is None:
            ensemble = "NA"

        try:
            buffer = BytesIO()
            fs.download_to_stream_by_name(rain_file, buffer)
            buffer.seek(0)
            rainfield, vtime = read_netcdf_buffer(buffer.read())
        except Exception as e:
            logging.warning(f"Could not load state file {rain_file}: {e}")
            continue

        records.append({
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

def make_metadata(rain:xr.DataArray, product:str, name:str, valid_time:datetime.datetime, base_time:Optional[datetime.datetime] = None, 
                  ensemble:Optional[int] = None ):

    """
    Generate the metadata dictionary to be written to the database.

    Args:
        rain (xr.DataArray): The rainfall field
        product (str): Product name (e.g., "qpe", "fx")
        name (str): Domain name
        valid_time (datetime): Valid time of the field (must be timezone-aware)
        base_time (datetime, optional): Forecast base time (timezone-aware)
        ensemble (int, optional): Ensemble member index

    Returns:
        dict: Metadata dictionary
    """
    war = np.count_nonzero(rain >= 1) / rain.size
    fx_lead_time = (valid_time - base_time).total_seconds() if base_time is not None else None 

    metadata = {
        "product": product,
        "domain": name,
        "ensemble": ensemble,
        "base_time": base_time,
        "valid_time": valid_time,
        "mean": float(rain.mean()),
        "wetted_area_ratio": float(war),
        "std_dev": float(rain.std()),
        "max": float(rain.max()),
        "forecast_lead_time": fx_lead_time 
    }
    return metadata 