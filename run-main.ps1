# FarndaCred Streamlit launcher — entry point is main.py (it loads app.py internally via get_loan_app).
# Usage from repo root:
#   .\run-main.ps1
#   .\run-main.ps1 --server.port 8502
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
python -m streamlit run main.py @args
