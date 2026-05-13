"""
01_download_era5.py
-------------------
Download ERA5 hourly single-level and pressure-level variables
over CONUS for a specified date range via the CDS API.

Requirements:
    pip install cdsapi

CDS credentials must be in ~/.cdsapirc:
    url: https://cds.climate.copernicus.eu/api/v2
    key: <UID>:<API-KEY>

Output:
    era5_raw/era5_YYYYMMDD.nc  (one file per day)
"""

import cdsapi
import os
from datetime import date, timedelta
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

# Date range (inclusive)
START_DATE = date(2023, 1, 1)
END_DATE   = date(2023, 1, 7)

# CONUS bounding box [N, W, S, E]  (CDS convention: north, west, south, east)
AREA = [55.0, -130.0, 20.0, -60.0]

# Variables to download
SINGLE_LEVEL_VARS = [
    "2m_temperature",
    "2m_dewpoint_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_pressure",
    "mean_sea_level_pressure",
    "total_precipitation",
]

# Hours (UTC) to pull — match RTMA availability (hourly)
HOURS = [f"{h:02d}:00" for h in range(24)]

OUTPUT_DIR = Path("era5_raw")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Download loop ──────────────────────────────────────────────────────────────

c = cdsapi.Client()

current = START_DATE
while current <= END_DATE:
    yyyy = current.strftime("%Y")
    mm   = current.strftime("%m")
    dd   = current.strftime("%d")
    tag  = current.strftime("%Y%m%d")
    outfile = OUTPUT_DIR / f"era5_{tag}.nc"

    if outfile.exists():
        print(f"[skip] {outfile} already exists")
        current += timedelta(days=1)
        continue

    print(f"[fetch] ERA5 {tag} ...")
    c.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": "reanalysis",
            "variable":     SINGLE_LEVEL_VARS,
            "year":         yyyy,
            "month":        mm,
            "day":          dd,
            "time":         HOURS,
            "area":         AREA,
            "format":       "netcdf",
        },
        str(outfile),
    )
    print(f"[done]  {outfile}")
    current += timedelta(days=1)

print("\nERA5 download complete.")
