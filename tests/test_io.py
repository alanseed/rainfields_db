# test/test_io.py

import pytest
import datetime
import numpy as np
import xarray as xr

import pytest
from pymongo.database import Database
from rainfields_db import get_db

from rainfields_db import (
    get_db,
    generate_coords,
    make_nc_name,
    write_netcdf_buffer,
    write_buffer_to_file,
    write_rainfield,
    make_metadata,
    get_rainfields_df,
)

@pytest.fixture
def test_db() -> Database:
    """
    Fixture that yields a connection to the test database and
    drops it after the test runs.
    """
    db = get_db()
    assert db.name == "test", "Expected to use test as test DB"

    yield db

    # Cleanup: drop the entire 'test' domain collections
    client = db.client
    prefix = "test.rain"

    for suffix in ["", ".files", ".chunks"]:
        coll_name = f"{prefix}{suffix}"
        if coll_name in db.list_collection_names():
            db.drop_collection(coll_name)

@pytest.mark.integration
def test_write_and_read_rainfields(test_db):
    config = {
        "domain": {
            "n_rows": 128,
            "n_cols": 128,
            "p_size": 2000,
            "start_x": 1627000,
            "start_y": 5854000
        },
        "projection": {
            "epsg": "EPSG:2193",
            "name": "transverse_mercator",
            "central_meridian": 173.0,
            "latitude_of_origin": 0.0,
            "scale_factor": 0.9996,
            "false_easting": 1600000.0,
            "false_northing": 10000000.0
        }
    }

    name = "test"
    product = "qpe"
    valid_time = datetime.datetime(2025, 7, 3, 1, 0, 0, tzinfo=datetime.timezone.utc)

    # Generate file name and coordinates
    file_name = make_nc_name(name, product, valid_time)
    x, y = generate_coords(config)
    n_rows = config["domain"]["n_rows"]
    n_cols = config["domain"]["n_cols"]

    # Create a synthetic rainfall field
    yy, xx = np.meshgrid(np.arange(n_rows), np.arange(n_cols), indexing='ij')
    cy = (n_rows - 1) / 2
    cx = (n_cols - 1) / 2
    rain_rate = np.sqrt((yy - cy) ** 2 + (xx - cx) ** 2)
    rain_da = xr.DataArray(
        rain_rate,
        dims=("y", "x"),
        coords={"x": x, "y": y},
        name="rainfall",
        attrs={
            "units": "mm/h",
            "long_name": "Rainfall intensity",
            "projection": config["projection"]["epsg"]
        }
    )

    # Write NetCDF to buffer
    nc_buffer = write_netcdf_buffer(rain_da, valid_time)
    write_buffer_to_file(nc_buffer, file_name)

    db = test_db

    # Write multiple timestamps to GridFS
    vtimes = []
    for ia in range(5):
        t_time = valid_time + ia * datetime.timedelta(minutes=10)
        vtimes.append(t_time)
        metadata = make_metadata(rain_da, product, name, t_time)
        filename = make_nc_name(name, product, t_time)
        write_rainfield(db, name, filename, nc_buffer, metadata)

    # Query and test
    query = {"metadata.product": product, "metadata.valid_time": {"$in": vtimes}}
    df = get_rainfields_df(db, name, query)

    assert len(df) == 5
    assert all(isinstance(vt, datetime.datetime) for vt in df["valid_time"])
    assert df["rainfield"].apply(lambda x: isinstance(x, xr.DataArray)).all()
    assert all(df["product"] == product)

    # Check that retrieved rainfall fields match the original
    for retrieved in df["rainfield"]:
        assert np.allclose(retrieved.values, rain_rate, atol=0.05), "Rainfield does not match original pattern"
