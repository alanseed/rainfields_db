"""
Test writing and reading necdf files to rainfields_db 
"""
from rainfields_db import get_db, generate_coords, make_nc_name
from rainfields_db import write_netcdf_buffer, write_nc_stream_to_file
from rainfields_db import write_rainfield, make_metadata
import datetime
import xarray as xr
import numpy as np

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

# Test make_nc_name
name = "test"
product = "fx"
valid_time = datetime.datetime(
    year=2025, month=7, day=3, hour=1, minute=0, second=0, tzinfo=datetime.UTC)
base_time = datetime.datetime(
    year=2025, month=7, day=3, hour=0, minute=0, second=0, tzinfo=datetime.UTC)
ens = 1
file_name = make_nc_name(name, product, valid_time, base_time, ens)
print(f"FX File name = {file_name}")

product = "qpe"
qpe_name = make_nc_name(name, product, valid_time)
print(f"QPE File name = {qpe_name}")

# Set up the xarray to write & read
x, y = generate_coords(config)
db = get_db()

n_rows = config["domain"]["n_rows"]
n_cols = config["domain"]["n_cols"]

# Create test pattern: distance to center in pixel units
yy, xx = np.meshgrid(np.arange(n_rows), np.arange(n_cols), indexing='ij')
cy = (n_rows - 1) / 2
cx = (n_cols - 1) / 2
rain_rate = np.sqrt((yy - cy)**2 + (xx - cx)**2)
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

nc_buffer = write_netcdf_buffer(rain_da, valid_time)
write_nc_stream_to_file(nc_buffer, qpe_name)
print(f"Written {qpe_name}")

metadata = make_metadata(rain_da, product, name, valid_time)
write_rainfield(db, name, qpe_name, nc_buffer, metadata)
