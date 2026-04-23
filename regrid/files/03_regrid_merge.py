"""
03_regrid_merge.py
------------------
Regrid ERA5 (0.25° lat/lon) and RTMA (2.5-km Lambert Conformal NDFD grid)
to a common CONUS lat/lon grid, then merge into a single xarray Dataset.

Strategy
--------
  Target grid : user-defined regular lat/lon at OUTPUT_RES degrees
  ERA5        : bilinear interpolation (already lat/lon, just resample)
  RTMA        : conservative or bilinear via xESMF from Lambert Conformal

Variable mapping (ERA5 name → RTMA GRIB shortName → merged name)
  2m_temperature         / t2m (TMP)  → temp_2m       [K]
  2m_dewpoint_temperature/ d2m (DPT)  → dewpt_2m      [K]
  10m_u_component_of_wind/ u10 (UGRD) → u10           [m/s]
  10m_v_component_of_wind/ v10 (VGRD) → v10           [m/s]
  surface_pressure        / sp  (PRES) → pres_sfc      [Pa]

Requirements
------------
    pip install xarray netCDF4 cfgrib xesmf numpy scipy tqdm

Output
------
    merged/merged_YYYYMMDD.nc
"""

import warnings
from datetime import date, timedelta
from pathlib import Path

import cfgrib
import numpy as np
import xarray as xr
import xesmf as xe
from tqdm import tqdm

warnings.filterwarnings("ignore")  # suppress ESMF deprecation noise

# ── Configuration ─────────────────────────────────────────────────────────────

START_DATE = date(2023, 1, 1)
END_DATE   = date(2023, 1, 7)

ERA5_DIR   = Path("era5_raw")
RTMA_DIR   = Path("rtma_raw")
OUT_DIR    = Path("merged")
OUT_DIR.mkdir(exist_ok=True)

# Target grid: CONUS lat/lon at OUTPUT_RES degrees
# 0.125° ≈ ~14 km  |  0.25° ≈ ~28 km  |  0.0625° ≈ ~7 km
OUTPUT_RES = 0.125

LAT_MIN, LAT_MAX = 20.0, 55.0
LON_MIN, LON_MAX = -130.0, -60.0

# RTMA GRIB2 variable filters  (cfgrib filter_by_keys)
# Each entry: (output_name, era5_varname, grib_filter)
VAR_MAP = [
    ("temp_2m",  "t2m",  {"typeOfLevel": "heightAboveGround", "level": 2,  "shortName": "2t"}),
    ("dewpt_2m", "d2m",  {"typeOfLevel": "heightAboveGround", "level": 2,  "shortName": "2d"}),
    ("u10",      "u10",  {"typeOfLevel": "heightAboveGround", "level": 10, "shortName": "10u"}),
    ("v10",      "v10",  {"typeOfLevel": "heightAboveGround", "level": 10, "shortName": "10v"}),
    ("pres_sfc", "sp",   {"typeOfLevel": "surface",           "level": 0,  "shortName": "sp"}),
]

# Regridding method: "bilinear" (fast) or "conservative" (flux-preserving)
REGRID_METHOD = "bilinear"

# ── Target grid ────────────────────────────────────────────────────────────────

target_lats = np.arange(LAT_MIN, LAT_MAX + OUTPUT_RES, OUTPUT_RES)
target_lons = np.arange(LON_MIN, LON_MAX + OUTPUT_RES, OUTPUT_RES)

ds_target = xr.Dataset(
    {
        "lat": (["lat"], target_lats, {"units": "degrees_north"}),
        "lon": (["lon"], target_lons, {"units": "degrees_east"}),
    }
)

# ── Helper: build xESMF regridder from a source Dataset ───────────────────────

_regridder_cache: dict = {}

def get_regridder(ds_src: xr.Dataset, tag: str) -> xe.Regridder:
    """Cache regridder by grid tag (expensive to recompute)."""
    if tag not in _regridder_cache:
        _regridder_cache[tag] = xe.Regridder(
            ds_src, ds_target, method=REGRID_METHOD, extrap_method="nearest_s2d"
        )
    return _regridder_cache[tag]

# ── Helper: open RTMA GRIB2 variable ──────────────────────────────────────────

def open_rtma_var(grib_path: Path, filter_keys: dict) -> xr.DataArray | None:
    """
    Open a single variable from an RTMA GRIB2 file using cfgrib.
    RTMA uses NDFD Lambert Conformal — cfgrib decodes lat/lon automatically.
    Returns a DataArray with dims (latitude, longitude) or None on failure.
    """
    try:
        ds = cfgrib.open_dataset(
            str(grib_path),
            filter_by_keys=filter_keys,
            backend_kwargs={"indexpath": ""},
        )
        # cfgrib returns dims named 'latitude'/'longitude' for projected grids
        var_name = list(ds.data_vars)[0]
        da = ds[var_name].squeeze(drop=True)
        # Rename so xESMF can find lat/lon coords
        if "latitude" in da.coords and "longitude" in da.coords:
            da = da.rename({"latitude": "lat", "longitude": "lon"})
        return da
    except Exception as e:
        print(f"    [cfgrib warn] {filter_keys.get('shortName','?')}: {e}")
        return None

# ── Helper: regrid ERA5 variable to target grid ────────────────────────────────

def regrid_era5_var(ds_era5: xr.Dataset, varname: str, time_idx: int) -> xr.DataArray | None:
    if varname not in ds_era5:
        return None
    da = ds_era5[varname].isel(time=time_idx)
    # ERA5 uses 'latitude'/'longitude' dim names
    da = da.rename({"latitude": "lat", "longitude": "lon"})
    # Longitude fix: ERA5 may use 0–360; convert to -180–180
    if float(da.lon.min()) >= 0:
        da = da.assign_coords(lon=(da.lon.values + 180) % 360 - 180)
        da = da.sortby("lon")
    regridder = get_regridder(da.to_dataset(name=varname), "era5")
    return regridder(da)

# ── Helper: regrid RTMA variable to target grid ────────────────────────────────

def regrid_rtma_var(da: xr.DataArray, var_tag: str) -> xr.DataArray:
    ds_src = da.to_dataset(name=var_tag)
    regridder = get_regridder(ds_src, f"rtma_{var_tag}")
    return regridder(da)

# ── Main processing loop ───────────────────────────────────────────────────────

current = START_DATE
while current <= END_DATE:
    tag     = current.strftime("%Y%m%d")
    outfile = OUT_DIR / f"merged_{tag}.nc"

    if outfile.exists():
        print(f"[skip] {outfile.name}")
        current += timedelta(days=1)
        continue

    print(f"\n[process] {tag}")

    # ── Open ERA5 day file ─────────────────────────────────────────────────────
    # CDS now delivers a zip archive containing two NetCDFs:
    #   data_stream-oper_stepType-instant.nc  (t2m, d2m, u10, v10, sp, msl)
    #   data_stream-oper_stepType-accum.nc    (tp)
    import zipfile, tempfile, shutil

    era5_zip = ERA5_DIR / f"era5_{tag}.nc"
    if not era5_zip.exists():
        print(f"  [missing] {era5_zip} — skipping")
        current += timedelta(days=1)
        continue

    def open_era5_zip(zip_path):
        """Extract CDS zip archive, open both NetCDFs, return merged Dataset."""
        tmpdir = Path(tempfile.mkdtemp(prefix="era5_"))
        try:
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(tmpdir)
            parts = []
            for nc in sorted(tmpdir.glob("*.nc")):
                parts.append(xr.open_dataset(nc, engine="netcdf4"))
            merged = xr.merge(parts)
            # Standardise time dim name: CDS uses valid_time
            if "valid_time" in merged.dims and "time" not in merged.dims:
                merged = merged.rename({"valid_time": "time"})
            # Load into memory so tmpdir can be cleaned up
            return merged.load()
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    ds_era5 = open_era5_zip(era5_zip)
    print(f"  [era5] vars={list(ds_era5.data_vars)} dims={dict(ds_era5.dims)}")
    n_times = ds_era5.dims["time"]

    # Collect hourly merged datasets
    hourly_datasets = []

    for t_idx in tqdm(range(n_times), desc=f"  hours", unit="hr"):
        tstamp = ds_era5.time.values[t_idx]

        # Hour string for RTMA filename
        import pandas as pd
        hour = pd.Timestamp(tstamp).hour
        rtma_path = RTMA_DIR / f"rtma_{tag}_{hour:02d}z.grb2_wexp"

        merged_vars = {}
        attrs_time  = {"time": tstamp}

        for out_name, era5_var, grib_filter in VAR_MAP:
            # ── ERA5 regrid ──
            era5_da = regrid_era5_var(ds_era5, era5_var, t_idx)

            # ── RTMA open + regrid ──
            rtma_da = None
            if rtma_path.exists():
                raw_rtma = open_rtma_var(rtma_path, grib_filter)
                if raw_rtma is not None:
                    rtma_da = regrid_rtma_var(raw_rtma, out_name)

            # ── Assemble ──
            # Drop GRIB scalar coords (heightAboveGround, step, etc.) that
            # conflict across variables before combining into one Dataset.
            GRIB_SCALARS = {"heightAboveGround", "heightAboveSea", "step",
                            "surface", "atmosphere", "number", "expver"}
            if era5_da is not None:
                era5_da = era5_da.drop_vars(
                    [c for c in era5_da.coords if c in GRIB_SCALARS], errors="ignore"
                )
                merged_vars[f"{out_name}_era5"] = era5_da.assign_coords(attrs_time)
            if rtma_da is not None:
                rtma_da = rtma_da.drop_vars(
                    [c for c in rtma_da.coords if c in GRIB_SCALARS], errors="ignore"
                )
                merged_vars[f"{out_name}_rtma"] = rtma_da.assign_coords(attrs_time)

        if not merged_vars:
            continue

        ds_hour = xr.Dataset(merged_vars)
        ds_hour = ds_hour.expand_dims("time")
        hourly_datasets.append(ds_hour)

    ds_era5.close()

    if not hourly_datasets:
        print(f"  [warn] no data assembled for {tag}")
        current += timedelta(days=1)
        continue

    # Concatenate hours → single daily file
    ds_day = xr.concat(hourly_datasets, dim="time")

    # ── Global attributes ──────────────────────────────────────────────────────
    ds_day.attrs.update({
        "title":       f"ERA5 + RTMA merged CONUS {tag}",
        "era5_source": "Copernicus CDS reanalysis-era5-single-levels",
        "rtma_source": "NCEI RTMA 2.5-km NDFD grid",
        "target_grid": f"{OUTPUT_RES}° lat/lon, CONUS",
        "regrid_method": REGRID_METHOD,
        "variables":   ", ".join(f"{n}_era5 / {n}_rtma" for n, *_ in VAR_MAP),
        "conventions": "CF-1.8",
    })

    # ── Coordinate attributes ──────────────────────────────────────────────────
    ds_day["lat"].attrs = {"units": "degrees_north", "long_name": "latitude",  "standard_name": "latitude"}
    ds_day["lon"].attrs = {"units": "degrees_east",  "long_name": "longitude", "standard_name": "longitude"}

    # Per-variable attributes
    var_attrs = {
        "temp_2m":  {"long_name": "2-m air temperature",     "units": "K"},
        "dewpt_2m": {"long_name": "2-m dewpoint temperature", "units": "K"},
        "u10":      {"long_name": "10-m U wind component",    "units": "m s-1"},
        "v10":      {"long_name": "10-m V wind component",    "units": "m s-1"},
        "pres_sfc": {"long_name": "surface pressure",         "units": "Pa"},
    }
    for out_name, attrs in var_attrs.items():
        for suffix in ("_era5", "_rtma"):
            vname = out_name + suffix
            if vname in ds_day:
                ds_day[vname].attrs = attrs

    # ── Write ──────────────────────────────────────────────────────────────────
    encoding = {
        v: {"zlib": True, "complevel": 4, "dtype": "float32"}
        for v in ds_day.data_vars
    }
    ds_day.to_netcdf(outfile, encoding=encoding)
    print(f"  [wrote] {outfile}  ({outfile.stat().st_size / 1e6:.1f} MB)")

    current += timedelta(days=1)

print("\nAll days processed.")
