# Contains write_netcdf_buffer, write_buffer_to_file, generate_coords, read_netcdf_buffer, make_nc_name
import io
import os
import numpy as np
import netCDF4
import tempfile
import datetime
import logging
import xarray as xr 
import pandas as pd
from pyproj import CRS 
from typing import Optional

def write_netcdf_buffer(rain: xr.DataArray, valid_time: datetime.datetime) -> io.BytesIO:
    """
    Write a NetCDF file from a 2D xarray.DataArray and a valid_time.
    Encodes rainfall as int16 with scale_factor and _FillValue.
    
    Parameters:
        rain: xarray.DataArray with dims ("y", "x") and coordinates "x", "y"
        valid_time: datetime.datetime in UTC

    Returns:
        io.BytesIO containing the NetCDF file
    """
    # Sanity checks
    assert "x" in rain.coords and "y" in rain.coords, "Missing 'x' or 'y' coordinates"
    assert rain.ndim == 2, "Expected 2D DataArray"

    if valid_time.tzinfo is None:
        valid_time = valid_time.replace(tzinfo=datetime.timezone.utc)
    else:
        valid_time = valid_time.astimezone(datetime.timezone.utc)

    x = rain.coords["x"].values
    y = rain.coords["y"].values

    projection = rain.attrs.get("projection", "EPSG:4326")
    units = rain.attrs.get("units", "mm/h")
    long_name = rain.attrs.get("long_name", "Rainfall rate")

    fill_value = -1
    scale_factor = 0.1
    add_offset = 0.0

    # Prepare data: scale, round, and convert to int16
    rain_data = np.nan_to_num(rain.values, nan=fill_value / scale_factor)
    rain_int16 = np.round(rain_data / scale_factor).astype(np.int16)
    rain_int16[rain_data == fill_value / scale_factor] = fill_value

    nc_bytes = b""
    tmp_path = None

    try:
        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmp_path = tmp.name

        with netCDF4.Dataset(tmp_path, mode="w", format="NETCDF4") as ds:
            # Dimensions
            ds.createDimension("y", len(y))
            ds.createDimension("x", len(x))
            ds.createDimension("time", 1)

            # Coordinate variables
            x_var = ds.createVariable("x", "f4", ("x",))
            y_var = ds.createVariable("y", "f4", ("y",))
            t_var = ds.createVariable("time", "i8", ("time",))

            x_var[:] = x
            y_var[:] = y
            t_var[0] = int(valid_time.timestamp())

            x_var.standard_name = "projection_x_coordinate"
            x_var.units = "m"
            y_var.standard_name = "projection_y_coordinate"
            y_var.units = "m"
            t_var.standard_name = "time"
            t_var.units = "seconds since 1970-01-01T00:00:00Z"

            # Rainfall variable
            rain_var = ds.createVariable(
                "rainfall", "i2", ("time", "y", "x"),
                zlib=True, complevel=5, fill_value=fill_value
            )
            rain_var[0, :, :] = rain_int16

            rain_var.scale_factor = scale_factor
            rain_var.add_offset = add_offset
            rain_var.units = units
            rain_var.long_name = long_name
            rain_var.grid_mapping = "projection"

            # Grid mapping
            crs = CRS.from_user_input(projection)
            cf_grid_mapping = crs.to_cf()
            proj_var = ds.createVariable("projection", "i4")
            for key, value in cf_grid_mapping.items():
                setattr(proj_var, key, value)

            # Global attributes
            ds.Conventions = "CF-1.7"
            ds.title = "Rainfall data"
            ds.institution = "Weather Radar New Zealand Ltd"

        # Read the file into memory
        with open(tmp_path, "rb") as f:
            nc_bytes = f.read()
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

    return io.BytesIO(nc_bytes)

def write_buffer_to_file(buffer: io.BytesIO, path: str) -> None:
    buffer.seek(0)
    with open(path, "wb") as f:
        f.write(buffer.read())


def generate_coords(config):
    """
    Generate x and y coordinate arrays from the configuration dictionary.

    Args:
        config (dict): Configuration dictionary expected to contain a 'domain' section with:
            - start_x (float)
            - start_y (float)
            - p_size (float)
            - n_rows (int)
            - n_cols (int)

    Returns:
        Tuple of two lists: (x, y) coordinate arrays

    Raises:
        KeyError: If required keys are missing in the config.
        TypeError/ValueError: If values are of incorrect type.
    """
    domain = config.get('domain')
    if domain is None or not isinstance(domain, dict):
        raise KeyError("Missing or malformed 'domain' section in config")

    required_keys = ['start_x', 'start_y', 'p_size', 'n_rows', 'n_cols']
    missing = [key for key in required_keys if key not in domain]
    if missing:
        raise KeyError(f"Missing required keys in domain config: {missing}")

    start_x = domain['start_x']
    start_y = domain['start_y']
    p_size = domain['p_size']
    n_rows = domain['n_rows']
    n_cols = domain['n_cols']

    try:
        x = [start_x + i * p_size for i in range(n_cols)]
        y = [start_y + i * p_size for i in range(n_rows)]
    except Exception as e:
        raise ValueError(f"Error generating coordinates: {e}")

    return x, y


def read_netcdf_buffer(buffer: bytes) -> tuple[xr.DataArray, datetime.datetime]:
    """
    Read a single-grid NetCDF file from a memory buffer and return it as a 2D xarray.DataArray,
    along with its valid time as a timezone-aware datetime.datetime object in UTC.

    :param buffer: Byte data of the NetCDF file from GridFS.
    :return: Tuple of (DataArray with dims y,x, valid_time as datetime.datetime in UTC)
    """
    byte_stream = io.BytesIO(buffer)

    with netCDF4.Dataset('inmemory', mode='r', memory=byte_stream.getvalue()) as ds:
        # Get spatial coords
        x = ds.variables["x"][:]
        y = ds.variables["y"][:]

        # Get rainfall data and clean it
        rain_rate = ds.variables["rainfall"][:]
        rain_rate = np.squeeze(rain_rate)
        rain_rate[rain_rate < 0] = np.nan

        # Convert time to datetime 
        time_var = ds.variables["time"]
        time_units = time_var.units  # e.g., "seconds since 1970-01-01 00:00:00"

        # netCDF4.num2date returns an array even for a single value — so slice with [:]
        time_val = netCDF4.num2date(time_var[0], units=time_units, only_use_cftime_datetimes=False)

        # Handle numpy.datetime64 or datetime.datetime
        if isinstance(time_val, np.datetime64):
            # Convert via pandas and set tz explicitly
            valid_time = pd.to_datetime(time_val).tz_localize("UTC").to_pydatetime()
        elif isinstance(time_val, datetime.datetime):
            valid_time = time_val if time_val.tzinfo else time_val.replace(tzinfo=datetime.timezone.utc)
        else:
            raise TypeError(f"Unexpected time object type: {type(time_val)}")

        # Try to extract projection from CF metadata
        projection = None
        if "projection" in ds.variables:
            proj_var = ds.variables["projection"]
            cf_dict = {attr: getattr(proj_var, attr) for attr in proj_var.ncattrs()}
            try:
                crs = CRS.from_cf(cf_dict)
                projection = crs.to_epsg() or crs.to_string()
            except Exception as e:
                logging.warning(f"Could not parse projection metadata: {e}")
                projection = "unknown"

        # Build DataArray
        rain_da = xr.DataArray(
            rain_rate,
            dims=("y", "x"),
            coords={"x": x, "y": y},
            name="rainfall",
            attrs={
                "units": getattr(ds.variables["rainfall"], "units", "mm/h"),
                "long_name": getattr(ds.variables["rainfall"], "long_name", "Rainfall intensity"),
                "projection": projection
            }
        )

    return rain_da, valid_time

def make_nc_name(name: str, prod: str, valid_time: datetime.datetime,
                 base_time: Optional[datetime.datetime] = None, ens: Optional[int] = None,
                 name_template: Optional[str] = None) -> str:
    """
    Generate a unique name for a single rain field using a formatting template.

    Default templates:
        Forecast products: "$N_$P_$V{%Y%m%dT%H%M%S}_$B{%Y%m%dT%H%M%S}_$E.nc"
        QPE products: "$N_$P_$V{%Y%m%dT%H%M%S}.nc"

    Where:
        $N = Domain name
        $P = Product name
        $V = Valid time (with strftime format)
        $B = Base time (with strftime format)
        $E = Ensemble number (zero-padded 2-digit)

    Returns:
        str: Unique NetCDF file name.
    """

    if not isinstance(valid_time, datetime.datetime):
        raise TypeError(f"valid_time must be datetime, got {type(valid_time)}")

    if base_time is not None and not isinstance(base_time, datetime.datetime):
        raise TypeError(f"base_time must be datetime or None, got {type(base_time)}")

    # Default template logic
    if name_template is None:
        name_template = "$N_$P_$V{%Y%m%dT%H%M%S}"
        if base_time is not None:
            name_template += "_$B{%Y%m%dT%H%M%S}"
        if ens is not None:
            name_template += "_$E"
        name_template += ".nc"

    result = name_template

    # Ensure timezone-aware times
    if valid_time.tzinfo is None:
        valid_time = valid_time.replace(tzinfo=datetime.timezone.utc)
    if base_time is not None and base_time.tzinfo is None:
        base_time = base_time.replace(tzinfo=datetime.timezone.utc)

    # Replace flags
    while "$" in result:
        flag_posn = result.find("$")
        if flag_posn == -1:
            break
        f_type = result[flag_posn + 1]

        try:
            if f_type in ['V', 'B']:
                field_start = result.find("{", flag_posn + 1)
                field_end = result.find("}", flag_posn + 1)
                if field_start == -1 or field_end == -1:
                    raise ValueError(f"Missing braces for format of '${f_type}' in template.")

                fmt = result[field_start + 1:field_end]
                if f_type == 'V':
                    time_str = valid_time.strftime(fmt)
                elif f_type == 'B' and base_time is not None:
                    time_str = base_time.strftime(fmt)
                else:
                    time_str = ""

                result = result[:flag_posn] + time_str + result[field_end + 1:]

            elif f_type == 'N':
                result = result[:flag_posn] + name + result[flag_posn + 2:]
            elif f_type == 'P':
                result = result[:flag_posn] + prod + result[flag_posn + 2:]
            elif f_type == 'E' and ens is not None:
                result = result[:flag_posn] + f"{ens:02d}" + result[flag_posn + 2:]
            else:
                raise ValueError(f"Unknown or unsupported flag '${f_type}' in template.")
        except Exception as e:
            raise ValueError(f"Error processing flag '${f_type}': {e}")

    return result
