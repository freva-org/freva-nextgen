"""Load data from a posix file system."""

from pathlib import Path
from typing import Union
import xarray as xr


def get_file_type(file_path):
    try:
        netCDF4.Dataset(file_path)
        return 'netcdf4'
    except:
        pass

    try:
        grbs = pygrib.open(file_path)
        grbs.close()
        return 'cfgrip'
    except:
        pass

    try:
        with rasterio.open(file_path) as src:
            if src.driver == 'GTiff':
                return 'GeoTIFF'
    except:
        pass

    try:
        with h5py.File(file_path, 'r') as file:
            return 'HDF5'
    except:
        pass

    return 'Unknown file type'

def load_data(inp_file: Union[str, Path]) -> xr.Dataset:
    """Open a dataset with xarray."""
    inp_file = Path(inp_file)
    file_type = inp_file.suffix.strip(".").lower()
    if file_type in ["netcdf",
