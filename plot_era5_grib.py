#!/usr/bin/env python3
"""
plot_era5_grib.py
-----------------
Plot a single field from an ERA5 GRIB2 file.

Requirements:
    mamba install -c conda-forge cfgrib eccodes xarray cartopy matplotlib

Usage examples:
    # List all variables/levels/times in the file
    python plot_era5_grib.py era5_pl_202006.grib2 --info

    # Plot 500 hPa geopotential, first time step (defaults)
    python plot_era5_grib.py era5_pl_202006.grib2

    # Plot 850 hPa temperature
    python plot_era5_grib.py era5_pl_202006.grib2 --var t --level 850

    # Plot a later time step (0-based index)
    python plot_era5_grib.py era5_pl_202006.grib2 --var t --level 850 --step 6

    # Save to PNG instead of interactive window
    python plot_era5_grib.py era5_pl_202006.grib2 --var t --level 850 --save
"""

import argparse
import sys
from pathlib import Path

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# ── Lookup tables ─────────────────────────────────────────────────────────────

UNIT_LABELS = {
    "z": "m² s⁻²",
    "t": "K",
    "u": "m s⁻¹",
    "v": "m s⁻¹",
    "q": "kg kg⁻¹",
    "w": "Pa s⁻¹",
}

DISPLAY_NAMES = {
    "z": "Geopotential",
    "t": "Temperature",
    "u": "U-wind",
    "v": "V-wind",
    "q": "Specific Humidity",
    "w": "Vertical Velocity (omega)",
}

# Possible dimension name variants used by cfgrib
LEVEL_DIMS = {"level", "isobaricInhPa", "pressure", "plev", "lev"}
LAT_DIMS   = {"latitude", "lat", "y"}
LON_DIMS   = {"longitude", "lon", "x"}
TIME_DIMS  = {"time", "valid_time", "step"}


def find_dim(da, candidates: set) -> str | None:
    """Return the first dim name that matches any candidate, or None."""
    for d in da.dims:
        if d in candidates or any(c in d.lower() for c in candidates):
            return d
    return None


# ── Info ──────────────────────────────────────────────────────────────────────

def list_info(filepath: str):
    import cfgrib
    print(f"\nInspecting: {filepath}\n")
    datasets = cfgrib.open_datasets(filepath)
    for i, ds in enumerate(datasets):
        print(f"--- Dataset {i} ---")
        print(f"  Variables : {list(ds.data_vars)}")
        for coord_name, coord in ds.coords.items():
            vals = coord.values.ravel()
            preview = vals[:6]
            suffix = "..." if len(vals) > 6 else ""
            print(f"  {coord_name:16s}: {preview}{suffix}  (n={len(vals)})")
        print()


# ── Plot ──────────────────────────────────────────────────────────────────────

def plot_field(filepath: str, varname: str, level: int,
               step_index: int, save: bool):

    import cfgrib
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    # Find the dataset containing this variable
    datasets = cfgrib.open_datasets(filepath)
    da = None
    for ds in datasets:
        if varname in ds.data_vars:
            da = ds[varname]
            break

    if da is None:
        available = [v for ds in datasets for v in ds.data_vars]
        print(f"ERROR: Variable '{varname}' not found.")
        print(f"Available: {sorted(set(available))}")
        sys.exit(1)

    print(f"Found '{varname}' with dims: {da.dims}, shape: {da.shape}")

    # ── Select level ─────────────────────────────────────────────────────────
    level_dim = find_dim(da, LEVEL_DIMS)
    if level_dim:
        available_levels = da.coords[level_dim].values.tolist()
        if level not in available_levels:
            print(f"ERROR: Level {level} hPa not found.")
            print(f"Available levels: {available_levels}")
            sys.exit(1)
        da = da.sel({level_dim: level})
        print(f"Selected level: {level} hPa")
    else:
        print("Note: no pressure level dimension found, using full field.")

    # ── Select time step ─────────────────────────────────────────────────────
    time_dim = find_dim(da, TIME_DIMS)
    if time_dim:
        n_steps = da.sizes[time_dim]
        if step_index >= n_steps:
            print(f"ERROR: --step {step_index} out of range "
                  f"(file has {n_steps} steps, 0-indexed).")
            sys.exit(1)
        da = da.isel({time_dim: step_index})
        print(f"Selected time step {step_index} of {n_steps}")

    # Readable time label
    time_label = ""
    for tc in ["valid_time", "time"]:
        if tc in da.coords:
            try:
                time_label = str(da.coords[tc].values)[:16]
            except Exception:
                pass
            break

    # ── Verify 2D ────────────────────────────────────────────────────────────
    if da.ndim != 2:
        print(f"ERROR: Still {da.ndim}D after selections, shape={da.shape}, dims={da.dims}")
        print("Try --info to see the full coordinate structure.")
        sys.exit(1)

    lat_dim = find_dim(da, LAT_DIMS) or da.dims[0]
    lon_dim = find_dim(da, LON_DIMS) or da.dims[1]

    lats = da.coords[lat_dim].values
    lons = da.coords[lon_dim].values
    data = da.values

    # ── Plot ─────────────────────────────────────────────────────────────────
    display_name = DISPLAY_NAMES.get(varname, varname)
    unit         = UNIT_LABELS.get(varname, "")

    fig, ax = plt.subplots(
        figsize=(13, 6),
        subplot_kw={"projection": ccrs.Robinson()}
    )

    cf = ax.pcolormesh(
        lons, lats, data,
        transform=ccrs.PlateCarree(),
        cmap="RdYlBu_r",
        shading="auto",
    )

    ax.add_feature(cfeature.COASTLINE, linewidth=0.6)
    ax.add_feature(cfeature.BORDERS,   linewidth=0.4, linestyle=":")
    ax.gridlines(draw_labels=True, linewidth=0.3, color="gray", alpha=0.5)

    level_str = f"{level} hPa  |  " if level_dim else ""
    ax.set_title(f"ERA5  {display_name}  |  {level_str}{time_label}", fontsize=11)

    cbar = fig.colorbar(cf, ax=ax, orientation="horizontal", pad=0.05, shrink=0.75)
    cbar.set_label(f"{display_name} ({unit})" if unit else display_name)

    plt.tight_layout()

    if save:
        stem    = Path(filepath).stem
        outname = f"{stem}_{varname}_{level}hPa_step{step_index:03d}.png"
        plt.savefig(outname, dpi=150, bbox_inches="tight")
        print(f"Saved: {outname}")
    else:
        plt.show()


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Plot a field from an ERA5 GRIB2 file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    p.add_argument("file",    help="Path to ERA5 GRIB2 file")
    p.add_argument("--var",   default="z",
                   help="Variable short name (default: z). Use --info to list all.")
    p.add_argument("--level", type=int, default=500,
                   help="Pressure level in hPa (default: 500)")
    p.add_argument("--step",  type=int, default=0,
                   help="Time step index, 0-based (default: 0)")
    p.add_argument("--save",  action="store_true",
                   help="Save as PNG instead of interactive display")
    p.add_argument("--info",  action="store_true",
                   help="List variables/levels/times then exit")
    return p.parse_args()


def main():
    args = parse_args()

    if not Path(args.file).exists():
        print(f"ERROR: File not found: {args.file}")
        sys.exit(1)

    if args.info:
        list_info(args.file)
        return

    plot_field(
        filepath   = args.file,
        varname    = args.var,
        level      = args.level,
        step_index = args.step,
        save       = args.save,
    )


if __name__ == "__main__":
    main()
