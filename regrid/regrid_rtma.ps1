# regrid_rtma.ps1
# Run from the directory containing rtma_raw\ and regrid_rtma_windows.py
# Usage:  .\regrid_rtma.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Activate conda env and run the Python script
conda run -n era5rtma python regrid_rtma_windows.py
