# rainfields_db/__init__.py
"""Public API for rainfields_db: GridFS + MongoDB helpers for rainfall fields."""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("rainfields_db")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

# --- nc_utils ---
from .core.nc_utils import (
    read_netcdf_buffer,
    write_netcdf_buffer,
    make_nc_name,
    generate_coords,
    write_buffer_to_file,
)

# --- db_utils ---
from .core.db_utils import (
    get_db,
    get_config,
    write_config,
)

# --- gridfs_io ---
from .core.gridfs_io import (
    get_rainfield,
    get_rainfields_df,
    get_state,
    get_states_df,
    write_rainfield,
    write_state,
    make_metadata,
)

# --- params_io ---
from .core.params_io import (
    get_param_docs,
    write_param_docs,
)

# --- stats_io ---
from .core.stats_io import (
    get_stats_docs,
    write_stats_docs,
)

__all__ = [
    # nc_utils
    "read_netcdf_buffer",
    "write_netcdf_buffer",
    "make_nc_name",
    "generate_coords",
    "write_buffer_to_file",
    # db_utils
    "get_db",
    "get_config",
    "write_config",
    # gridfs_io
    "get_rainfield",
    "get_rainfields_df",
    "get_state",
    "get_states_df",
    "write_rainfield",
    "write_state",
    "make_metadata",
    # params_io
    "get_param_docs",
    "write_param_docs",
    # stats_io
    "get_stats_docs",
    "write_stats_docs",
]

