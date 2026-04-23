#!/usr/bin/env python3
"""
download_era5_surface_nc.py
---------------------------
Download ERA5 single-level (surface) variables in NetCDF format,
6-hourly, cropped to CONUS.

Requirements:
    mamba install -c conda-forge cdsapi

CDS credentials (one-time setup):
    Create C:\\Users\\<yourname>\\.cdsapirc with:
        url: https://cds.climate.copernicus.eu/api
        key: <YOUR-API-KEY>
    Get your key at: https://cds.climate.copernicus.eu (login -> profile)

Usage examples:
    # Download June 2020 (default)
    python download_era5_surface_nc.py --year 2020 --month 6

    # Download multiple months
    python download_era5_surface_nc.py --year 2020 --month 6 7 8

    # Custom output directory
    python download_era5_surface_nc.py --year 2020 --month 6 --outdir C:/data/era5

    # Dry run — preview request without submitting
    python download_era5_surface_nc.py --year 2020 --month 6 --dry-run
"""

import argparse
import sys
import calendar
from pathlib import Path

import warnings
warnings.filterwarnings("ignore")

# ── Configuration ─────────────────────────────────────────────────────────────

# Surface variables to download
VARIABLES = [
    "2m_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "mean_sea_level_pressure",
    "total_precipitation",
]

# 6-hourly time steps
TIMES = ["00:00", "06:00", "12:00", "18:00"]

# CONUS bounding box: North, West, South, East
CONUS_AREA = [55, -130, 20, -60]


# ── Helpers ───────────────────────────────────────────────────────────────────

def days_in_month(year: int, month: int) -> list:
    n = calendar.monthrange(year, month)[1]
    return [f"{d:02d}" for d in range(1, n + 1)]


def build_request(year: int, month: int, outdir: str) -> tuple:
    year_str  = str(year)
    month_str = f"{month:02d}"

    request = {
        "product_type": "reanalysis",
        "variable":     VARIABLES,
        "year":         year_str,
        "month":        month_str,
        "day":          days_in_month(year, month),
        "time":         TIMES,
        "area":         CONUS_AREA,
        "format":       "netcdf",
    }

    fname   = f"era5_surface_conus_{year_str}{month_str}.nc"
    outpath = str(Path(outdir) / fname)
    return request, outpath


def download_month(client, request: dict, outpath: str, dry_run: bool):
    print(f"\n{'[DRY RUN] ' if dry_run else ''}-> {outpath}")
    print(f"  Variables : {request['variable']}")
    print(f"  Period    : {request['year']}-{request['month']}-01 "
          f"to {request['year']}-{request['month']}-{request['day'][-1]}")
    print(f"  Times     : {request['time']}")
    print(f"  Area      : N={CONUS_AREA[0]} W={CONUS_AREA[1]} "
          f"S={CONUS_AREA[2]} E={CONUS_AREA[3]}")
    print(f"  Format    : NetCDF")

    if dry_run:
        return

    Path(outpath).parent.mkdir(parents=True, exist_ok=True)
    client.retrieve("reanalysis-era5-single-levels", request, outpath)
    size_mb = Path(outpath).stat().st_size / 1e6
    print(f"  Saved: {size_mb:.1f} MB -> {outpath}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Download ERA5 surface NetCDF files (6-hourly, CONUS).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    p.add_argument("--year",  type=int, required=True,
                   help="Year (e.g. 2020)")
    p.add_argument("--month", type=int, nargs="+", required=True,
                   help="Month(s), space-separated (e.g. 6 7 8)")
    p.add_argument("--outdir", type=str, default=".",
                   help="Output directory (default: current directory)")
    p.add_argument("--dry-run", action="store_true",
                   help="Preview request without submitting")
    return p.parse_args()


def main():
    args = parse_args()

    if not args.dry_run:
        try:
            import cdsapi
            client = cdsapi.Client()
        except ImportError:
            print("ERROR: cdsapi not installed. Run: mamba install -c conda-forge cdsapi")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: Could not initialize CDS client: {e}")
            print("Check your .cdsapirc credentials file.")
            sys.exit(1)
    else:
        client = None

    print(f"\nERA5 surface NetCDF download -- CONUS -- {args.year}")
    print(f"Months: {args.month}")

    for month in sorted(args.month):
        request, outpath = build_request(args.year, month, args.outdir)

        p = Path(outpath)
        if p.exists() and p.stat().st_size > 0 and not args.dry_run:
            print(f"\n  [SKIP] {outpath} already exists ({p.stat().st_size/1e6:.1f} MB)")
            continue

        download_month(client, request, outpath, args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
