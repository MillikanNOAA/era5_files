#!/usr/bin/env python3
"""
download_era5_grib.py
---------------------
Download ERA5 data in GRIB2 format from the Copernicus Climate Data Store (CDS).

Requirements:
    pip install cdsapi

CDS credentials setup (one-time):
    Linux/macOS: Create ~/.cdsapirc
    Windows:     Create C:\\Users\\<yourname>\\.cdsapirc
    Contents:
        url: https://cds.climate.copernicus.eu/api
        key: <YOUR-API-KEY>
    Get your key at: https://cds.climate.copernicus.eu (login → profile)

Usage examples:
    # Single-level surface variables, one month
    python download_era5_grib.py --dataset single-levels --year 2020 --month 6

    # Pressure-level variables, multiple months
    python download_era5_grib.py --dataset pressure-levels --year 2020 --month 1 2 3

    # Custom output directory
    python download_era5_grib.py --dataset single-levels --year 2020 --month 6 \\
        --outdir /data/era5

    # Custom bounding box (N W S E)
    python download_era5_grib.py --dataset pressure-levels --year 2020 --month 6 \\
        --area 55 -130 20 -60

    # 6-hourly only (much smaller files)
    python download_era5_grib.py --dataset pressure-levels --year 2020 --month 6 \\
        --time 00:00 06:00 12:00 18:00

    # Dry run — preview request without submitting
    python download_era5_grib.py --dataset pressure-levels --year 2020 --month 6 --dry-run
"""

import argparse
import sys
import calendar
from pathlib import Path


# ── Variable presets ──────────────────────────────────────────────────────────

SINGLE_LEVEL_VARS = [
    "2m_temperature",
    "2m_dewpoint_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "mean_sea_level_pressure",
    "surface_pressure",
    "total_precipitation",
    "total_column_water_vapour",
]

PRESSURE_LEVEL_VARS = [
    "temperature",
    "u_component_of_wind",
    "v_component_of_wind",
    "geopotential",
    "specific_humidity",
    "vertical_velocity",
]

# hPa levels — subset most relevant for ML downscaling inputs
PRESSURE_LEVELS = [
    "500", "700", "850", "925", "1000"
]

# All available hours — pass --time 00:00 06:00 12:00 18:00 for 6-hourly
HOURS = [f"{h:02d}:00" for h in range(24)]


# ── Helpers ───────────────────────────────────────────────────────────────────

def days_in_month(year: int, month: int) -> list:
    """Return zero-padded day strings for a given year/month."""
    n = calendar.monthrange(year, month)[1]
    return [f"{d:02d}" for d in range(1, n + 1)]


def build_request(args, month: int):
    """Build (dataset_id, request_dict, output_path) for one month."""
    year_str  = str(args.year)
    month_str = f"{month:02d}"
    days      = days_in_month(args.year, month)

    base_request = {
        "product_type": "reanalysis",
        "year":         year_str,
        "month":        month_str,
        "day":          days,
        "time":         args.time if args.time else HOURS,
        "format":       "grib",
    }

    if args.area:
        # CDS area order: North, West, South, East
        base_request["area"] = args.area

    if args.dataset == "pressure-levels":
        dataset_id = "reanalysis-era5-pressure-levels"
        base_request["variable"]       = args.variables if args.variables else PRESSURE_LEVEL_VARS
        base_request["pressure_level"] = args.levels if args.levels else PRESSURE_LEVELS
        tag = "pl"
    else:
        dataset_id = "reanalysis-era5-single-levels"
        base_request["variable"] = args.variables if args.variables else SINGLE_LEVEL_VARS
        tag = "sl"

    fname   = f"era5_{tag}_{year_str}{month_str}.grib2"
    outpath = str(Path(args.outdir) / fname)

    return dataset_id, base_request, outpath


def download_month(client, dataset_id: str, request: dict,
                   outpath: str, dry_run: bool) -> None:
    """Submit one CDS request and save to outpath."""
    print(f"\n{'[DRY RUN] ' if dry_run else ''}-> {outpath}")
    print(f"  Dataset : {dataset_id}")
    print(f"  Year    : {request['year']}  Month: {request['month']}")
    print(f"  Days    : {request['day'][0]}-{request['day'][-1]}")
    print(f"  Hours   : {len(request['time'])}x "
          f"({request['time'][0]}-{request['time'][-1]})")
    if "pressure_level" in request:
        print(f"  Levels  : {request['pressure_level']}")
    print(f"  Vars    : {request['variable']}")
    if "area" in request:
        print(f"  Area    : N={request['area'][0]} W={request['area'][1]} "
              f"S={request['area'][2]} E={request['area'][3]}")

    if dry_run:
        return

    Path(outpath).parent.mkdir(parents=True, exist_ok=True)
    client.retrieve(dataset_id, request, outpath)
    size_mb = Path(outpath).stat().st_size / 1e6
    print(f"  Saved  {size_mb:.1f} MB -> {outpath}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Download ERA5 GRIB2 files from the Copernicus CDS.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    p.add_argument("--dataset", choices=["single-levels", "pressure-levels"],
                   default="pressure-levels",
                   help="ERA5 product type (default: pressure-levels)")

    p.add_argument("--year", type=int, required=True,
                   help="Year to download (e.g. 2020)")

    p.add_argument("--month", type=int, nargs="+", required=True,
                   help="Month(s) to download, space-separated (e.g. 1 2 3)")

    p.add_argument("--outdir", type=str, default=".",
                   help="Output directory (default: current directory)")

    p.add_argument("--area", type=float, nargs=4,
                   metavar=("N", "W", "S", "E"),
                   help="Bounding box crop: N W S E (e.g. 55 -130 20 -60 for CONUS)")

    p.add_argument("--time", type=str, nargs="+",
                   help="Hours to download (default: all 24). "
                        "Example: --time 00:00 06:00 12:00 18:00")

    p.add_argument("--levels", type=str, nargs="+",
                   help=f"Pressure levels in hPa (default: {PRESSURE_LEVELS}). "
                        "Only used with --dataset pressure-levels.")

    p.add_argument("--variables", type=str, nargs="+",
                   help="Override default variable list.")

    p.add_argument("--dry-run", action="store_true",
                   help="Print request details without submitting to CDS.")

    return p.parse_args()


def main():
    args = parse_args()

    if not args.dry_run:
        try:
            import cdsapi
            client = cdsapi.Client()
        except ImportError:
            print("ERROR: cdsapi not installed. Run: pip install cdsapi")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: Could not initialize CDS client: {e}")
            print("Check that ~/.cdsapirc (Linux/macOS) or "
                  "C:\\Users\\<you>\\.cdsapirc (Windows) exists with valid credentials.")
            sys.exit(1)
    else:
        client = None

    print(f"\nERA5 GRIB2 download -- {args.dataset} -- {args.year}")
    print(f"Months requested: {args.month}")

    for month in sorted(args.month):
        dataset_id, request, outpath = build_request(args, month)

        # Skip if file already exists and is non-empty
        p = Path(outpath)
        if p.exists() and p.stat().st_size > 0 and not args.dry_run:
            print(f"\n  [SKIP] {outpath} already exists "
                  f"({p.stat().st_size/1e6:.1f} MB)")
            continue

        download_month(client, dataset_id, request, outpath, args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()