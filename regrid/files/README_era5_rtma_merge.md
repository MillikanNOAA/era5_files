# ERA5 + RTMA Regrid & Merge — CONUS Exploratory Dataset

## Overview

| Dataset | Native grid | Resolution | Projection |
|---------|------------|------------|------------|
| ERA5    | Regular lat/lon | 0.25° (~28 km) | Plate carrée |
| RTMA    | NDFD | 2.5 km | Lambert Conformal Conic |
| **Target** | **Regular lat/lon** | **0.125° (~14 km)** | **Plate carrée** |

ERA5 is bilinearly resampled from 0.25° to 0.125°.  
RTMA is reprojected from Lambert Conformal to lat/lon at 0.125° using xESMF.

---

## 1. Environment Setup

```bash
# Create a dedicated conda env
conda create -n era5rtma python=3.11 -y
conda activate era5rtma

# Core science stack
conda install -c conda-forge xarray netcdf4 scipy numpy tqdm -y

# xESMF requires ESMF — conda-forge is the only reliable path
conda install -c conda-forge xesmf esmpy -y

# GRIB2 reading
pip install cfgrib eccodes

# ERA5 download
pip install cdsapi

# HTTP download (RTMA)
pip install requests tqdm
```

> **Note on xESMF:** Do not install via pip alone. The underlying ESMF library
> must be compiled with the correct MPI backend; conda-forge handles this
> automatically. On NOAA RDHPCS load the ESMF module or build inside Singularity.

---

## 2. CDS API Credentials (ERA5)

Create `~/.cdsapirc`:

```
url: https://cds.climate.copernicus.eu/api/v2
key: <your-UID>:<your-API-key>
```

Keys are at: https://cds.climate.copernicus.eu/user/<your-username>

---

## 3. Configure Date Range and Variables

Edit the `# ── Configuration ──` block at the top of each script:

| Parameter | Script | Description |
|-----------|--------|-------------|
| `START_DATE` / `END_DATE` | 01, 02, 03 | Date range (must match across all three) |
| `SINGLE_LEVEL_VARS` | 01 | ERA5 variables to download |
| `TARGET_HOURS` | 02 | UTC hours to pull from RTMA |
| `OUTPUT_RES` | 03 | Target grid spacing in degrees |
| `REGRID_METHOD` | 03 | `"bilinear"` or `"conservative"` |
| `VAR_MAP` | 03 | ERA5↔RTMA variable pairing |

---

## 4. Run the Scripts in Order

```bash
# Step 1: Download ERA5 (queues on CDS — may take minutes to hours per day)
python 01_download_era5.py

# Step 2: Download RTMA (direct HTTP — typically fast)
python 02_download_rtma.py

# Step 3: Regrid and merge
python 03_regrid_merge.py
```

Expected directory structure after completion:

```
era5_raw/
    era5_20230101.nc
    era5_20230102.nc
    ...
rtma_raw/
    rtma_20230101_00z.grib2
    rtma_20230101_01z.grib2
    ...
merged/
    merged_20230101.nc
    merged_20230102.nc
    ...
```

---

## 5. Regridding Details

### Why xESMF?

| Method | When to use |
|--------|-------------|
| `bilinear` | Smooth fields: temperature, dewpoint, wind, pressure. Fast. |
| `conservative` | Flux/area quantities: precipitation, radiation. Preserves totals. |
| `nearest_s2d` | Used as extrapolation fallback at grid edges only. |

### RTMA Projection

RTMA CONUS 2.5-km uses the **NDFD Lambert Conformal Conic** projection:

```
Standard parallels : 25.0°N, 25.0°N
Central meridian   : 265.0°E (= -95°W)
Reference lat      : 25.0°N
NW corner          : 19.229°N, 233.723°E
Grid size          : 2345 × 1597 points
```

`cfgrib` decodes the 2D lat/lon arrays automatically from the GRIB metadata,
so you do not need to reproject manually — xESMF handles the rest given those
lat/lon arrays.

### ERA5 Longitude Convention

ERA5 from CDS uses –180 to +180. If you download from an alternate endpoint
that returns 0–360, `03_regrid_merge.py` auto-converts. If you see a
`longitude` seam artifact over –130° or –60° it is a sign of a mixed
convention; fix by sorting after assignment.

---

## 6. Inspect the Output

```python
import xarray as xr

ds = xr.open_dataset("merged/merged_20230101.nc")
print(ds)

# Compare ERA5 vs RTMA temperature at a point
import matplotlib.pyplot as plt

lat, lon = 40.0, -105.0  # Denver-ish
t_era5 = ds["temp_2m_era5"].sel(lat=lat, lon=lon, method="nearest")
t_rtma = ds["temp_2m_rtma"].sel(lat=lat, lon=lon, method="nearest")

plt.figure(figsize=(10, 4))
plt.plot(t_era5.time, t_era5 - 273.15, label="ERA5")
plt.plot(t_rtma.time, t_rtma - 273.15, label="RTMA")
plt.ylabel("2-m Temperature (°C)")
plt.legend()
plt.title("ERA5 vs RTMA — Denver 2023-01-01")
plt.tight_layout()
plt.savefig("comparison.png", dpi=150)
```

---

## 7. Known Issues and Workarounds

### RTMA archive gaps
NCEI RTMA coverage starts ~2011 but has gaps. If `02_download_rtma.py` reports
missing files, check the NCEI inventory manually:
```
https://www.ncei.noaa.gov/data/real-time-mesoscale-analysis/access/YYYY/MM/
```

### RTMA for recent data (last ~2 days)
NCEI does not yet have recent files. Use NOMADS instead:
```
https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtma/prod/rtma2p5.YYYYMMDD/
```

### xESMF weight file reuse
On repeated runs the regridder rebuilds weight files if not cached. To persist:
```python
regridder = xe.Regridder(ds_src, ds_target, method="bilinear",
                         weights="weights_rtma_to_target.nc",
                         reuse_weights=True)
```

### SLURM / HPC
For multi-month processing, parallelize by day:
```bash
#SBATCH --array=0-364
python 03_regrid_merge.py --day-offset $SLURM_ARRAY_TASK_ID
```
(Requires minor refactor to accept `--day-offset` arg.)

---

## 8. Variable Reference

| Merged name | ERA5 var | RTMA shortName | Units | Notes |
|-------------|----------|----------------|-------|-------|
| `temp_2m`   | `t2m`    | `2t`           | K     | |
| `dewpt_2m`  | `d2m`    | `2d`           | K     | |
| `u10`       | `u10`    | `10u`          | m/s   | positive = westerly |
| `v10`       | `v10`    | `10v`          | m/s   | positive = southerly |
| `pres_sfc`  | `sp`     | `sp`           | Pa    | RTMA uses surface; ERA5 uses `sp` |
