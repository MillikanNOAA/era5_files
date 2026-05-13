"""
02_download_rtma.py
-------------------
Download RTMA (Real-Time Mesoscale Analysis) 2.5-km CONUS GRIB2 files
from the NOAA public AWS S3 bucket (no credentials needed, no rate limit).

Primary source (AWS S3, all historical data):
    https://noaa-rtma-pds.s3.amazonaws.com/rtma2p5.YYYYMMDD/rtma2p5.tHHz.2dvaranl_ndfd.grb2_wexp

Fallback source (NOMADS, last ~2 days only):
    https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtma/prod/rtma2p5.YYYYMMDD/rtma2p5.tHHz.2dvaranl_ndfd.grb2_wexp

Key filename detail: the correct suffix is  .grb2_wexp  (not .grb2).
cfgrib reads _wexp files identically to plain .grb2.

RTMA variables in the 2dvaranl_ndfd product:
    TMP   – 2-m temperature
    DPT   – 2-m dewpoint
    UGRD  – 10-m U wind component
    VGRD  – 10-m V wind component
    PRES  – surface pressure
    WIND  – 10-m wind speed
    TCDC  – total cloud cover
    VIS   – visibility
The full file contains all variables; filter at regrid step with cfgrib.

Requirements:
    pip install requests tqdm

Output:
    rtma_raw/rtma_YYYYMMDD_HHz.grb2_wexp
"""

import requests
import sys
from datetime import date, timedelta
from pathlib import Path
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────────────

START_DATE = date(2023, 1, 1)
END_DATE   = date(2023, 1, 7)

# Hours (UTC) to pull — match ERA5 selection
TARGET_HOURS = list(range(24))

# Source priority: AWS S3 first (full archive), NOMADS as fallback
AWS_BASE    = "https://noaa-rtma-pds.s3.amazonaws.com"
NOMADS_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtma/prod"

OUTPUT_DIR = Path("rtma_raw")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Helpers ────────────────────────────────────────────────────────────────────

def build_urls(dt: date, hour: int) -> list[str]:
    """
    Return candidate URLs in priority order (AWS first, NOMADS fallback).
    Correct filename suffix is _wexp.
    """
    tag  = dt.strftime("%Y%m%d")
    hstr = f"{hour:02d}"
    fname = f"rtma2p5.t{hstr}z.2dvaranl_ndfd.grb2_wexp"
    subdir = f"rtma2p5.{tag}"
    return [
        f"{AWS_BASE}/{subdir}/{fname}",
        f"{NOMADS_BASE}/{subdir}/{fname}",
    ]

def download_file(url: str, dest: Path) -> bool:
    """Stream-download url → dest. Returns True on success."""
    try:
        r = requests.get(url, stream=True, timeout=120)
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True,
            desc=dest.name, leave=False
        ) as bar:
            for chunk in r.iter_content(chunk_size=1 << 16):
                f.write(chunk)
                bar.update(len(chunk))
        return True
    except requests.HTTPError as e:
        print(f"  [HTTP {e.response.status_code}] {url}")
        return False
    except Exception as e:
        print(f"  [error] {e}")
        return False

# ── Download loop ──────────────────────────────────────────────────────────────

current = START_DATE
missing = []

while current <= END_DATE:
    for hour in TARGET_HOURS:
        tag     = current.strftime("%Y%m%d")
        hstr    = f"{hour:02d}"
        # Output filename mirrors the real extension so cfgrib can read it
        outfile = OUTPUT_DIR / f"rtma_{tag}_{hstr}z.grb2_wexp"

        if outfile.exists():
            continue

        print(f"[fetch] {outfile.name}")
        success = False
        for url in build_urls(current, hour):
            ok = download_file(url, outfile)
            if ok:
                success = True
                break
            # Remove partial file before trying next source
            if outfile.exists():
                outfile.unlink()

        if not success:
            print(f"  [all sources failed] {tag} {hstr}z")
            missing.append((current, hour))

    current += timedelta(days=1)

print(f"\nRTMA download complete.")
if missing:
    print(f"Missing ({len(missing)} files):")
    for d, h in missing:
        print(f"  {d.strftime('%Y%m%d')} {h:02d}z")
    sys.exit(1)
