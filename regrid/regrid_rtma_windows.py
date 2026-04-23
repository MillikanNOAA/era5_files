"""
regrid_rtma_windows.py
----------------------
Regrid all RTMA .grb2_wexp files in rtma_raw\ onto the ERA5 0.25-degree
CONUS lat/lon grid and write NetCDF4 output to rtma_regridded\.

Uses scipy bilinear interpolation — no CDO required.
Equivalent to:  cdo -f nc4 remapbil,target_grid.txt input.grb2 output.nc

Requirements:
    conda install -c conda-forge xarray scipy netcdf4 cfgrib tqdm
"""

from pathlib import Path
import numpy as np
import xarray as xr
import cfgrib
from scipy.interpolate import RegularGridInterpolator
from tqdm import tqdm

# ── Target grid (matches ERA5 / target_grid.txt) ──────────────────────────────
LAT_MIN, LAT_MAX, LAT_N = 20.0,  55.0,  141
LON_MIN, LON_MAX, LON_N = -130.0, -60.0, 281

target_lats = np.linspace(LAT_MAX, LAT_MIN, LAT_N)   # north → south
target_lons = np.linspace(LON_MIN, LON_MAX, LON_N)

# ── Paths ─────────────────────────────────────────────────────────────────────
INDIR  = Path("rtma_raw")
OUTDIR = Path("rtma_regridded")
OUTDIR.mkdir(exist_ok=True)

# RTMA variables to extract (shortName → output name)
VARS = {
    "2t":  ("temp_2m",  {"typeOfLevel": "heightAboveGround", "level": 2}),
    "2d":  ("dewpt_2m", {"typeOfLevel": "heightAboveGround", "level": 2}),
    "10u": ("u10",      {"typeOfLevel": "heightAboveGround", "level": 10}),
    "10v": ("v10",      {"typeOfLevel": "heightAboveGround", "level": 10}),
    "sp":  ("pres_sfc", {"typeOfLevel": "surface",           "level": 0}),
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def open_rtma_var(path: Path, short_name: str, filter_keys: dict):
    """Open one variable from an RTMA GRIB2 file. Returns (data, lat2d, lon2d)."""
    fkeys = {"shortName": short_name, **filter_keys}
    try:
        ds = cfgrib.open_dataset(
            str(path),
            filter_by_keys=fkeys,
            backend_kwargs={"indexpath": ""},
        )
        var = list(ds.data_vars)[0]
        da  = ds[var].squeeze(drop=True)
        lat = da.latitude.values   # 2D (y, x)
        lon = da.longitude.values  # 2D (y, x), 0–360
        lon = np.where(lon > 180, lon - 360, lon)   # → -180–180
        return da.values, lat, lon
    except Exception as e:
        print(f"    [skip] {short_name}: {e}")
        return None, None, None


def bilinear_regrid(data2d, src_lat2d, src_lon2d, tgt_lats, tgt_lons):
    """
    Regrid data2d (y, x) with 2D src lat/lon onto a regular tgt lat/lon grid.

    Strategy: flatten source points, use scipy LinearNDInterpolator
    (equivalent to bilinear for scattered→regular).
    Falls back gracefully for points outside the source domain (NaN).
    """
    from scipy.interpolate import LinearNDInterpolator

    src_points = np.column_stack([src_lat2d.ravel(), src_lon2d.ravel()])
    src_values = data2d.ravel()

    # Remove NaN source points
    valid = np.isfinite(src_values)
    interp = LinearNDInterpolator(src_points[valid], src_values[valid])

    tgt_lon2d, tgt_lat2d = np.meshgrid(tgt_lons, tgt_lats)
    tgt_points = np.column_stack([tgt_lat2d.ravel(), tgt_lon2d.ravel()])

    result = interp(tgt_points).reshape(len(tgt_lats), len(tgt_lons))
    return result.astype(np.float32)

# ── Main loop ─────────────────────────────────────────────────────────────────

files = sorted(INDIR.glob("*.grb2_wexp"))
print(f"Found {len(files)} RTMA files in {INDIR}\n")

for fpath in tqdm(files, unit="file"):
    outpath = OUTDIR / (fpath.stem + ".nc")
    if outpath.exists():
        continue

    data_vars = {}
    timestamp = None

    for short_name, (out_name, filter_keys) in VARS.items():
        values, lat2d, lon2d = open_rtma_var(fpath, short_name, filter_keys)
        if values is None:
            continue

        regridded = bilinear_regrid(values, lat2d, lon2d, target_lats, target_lons)
        data_vars[out_name] = (["latitude", "longitude"], regridded)

        if timestamp is None:
            # Parse timestamp from filename: rtma_YYYYMMDD_HHz
            stem = fpath.stem  # e.g. rtma_20230101_00z
            parts = stem.split("_")
            date_str = parts[1]
            hour_str = parts[2].replace("z", "")
            timestamp = np.datetime64(
                f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T{hour_str.zfill(2)}:00"
            )

    if not data_vars:
        tqdm.write(f"  [warn] no variables extracted from {fpath.name}")
        continue

    ds_out = xr.Dataset(
        data_vars,
        coords={
            "latitude":  ("latitude",  target_lats),
            "longitude": ("longitude", target_lons),
            "time":      timestamp,
        },
    )

    # Attributes
    ds_out["latitude"].attrs  = {"units": "degrees_north", "standard_name": "latitude"}
    ds_out["longitude"].attrs = {"units": "degrees_east",  "standard_name": "longitude"}
    ds_out.attrs = {
        "title":       f"RTMA regridded to ERA5 0.25° CONUS grid",
        "source":      str(fpath.name),
        "regrid":      "scipy LinearNDInterpolator (bilinear equivalent)",
        "conventions": "CF-1.8",
    }

    var_attrs = {
        "temp_2m":  {"long_name": "2-m air temperature",     "units": "K"},
        "dewpt_2m": {"long_name": "2-m dewpoint temperature", "units": "K"},
        "u10":      {"long_name": "10-m U wind component",    "units": "m s-1"},
        "v10":      {"long_name": "10-m V wind component",    "units": "m s-1"},
        "pres_sfc": {"long_name": "surface pressure",         "units": "Pa"},
    }
    for v, attrs in var_attrs.items():
        if v in ds_out:
            ds_out[v].attrs = attrs

    encoding = {v: {"zlib": True, "complevel": 4} for v in ds_out.data_vars}
    ds_out.to_netcdf(outpath, encoding=encoding)

print("\nDone.")
