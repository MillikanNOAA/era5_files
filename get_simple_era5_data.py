import cdsapi
# import zipfile
import os

c = cdsapi.Client()

c.retrieve(
    "reanalysis-era5-single-levels",
    {
        "product_type": "reanalysis",
        "variable": [
            "2m_temperature",           # instant
            "mean_sea_level_pressure",  # instant
            "total_precipitation",      # accum
            "10m_u_component_of_wind",  # instant
        ],
        "year": "2020",
        "month": "01",
        "day": "01",
        "time": "00:00",
        "format": "netcdf",
    },
    "era5_4vars.nc"
)
