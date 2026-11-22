# Build mme wheel for Fabric Environment upload
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

python -m pip install --upgrade build setuptools wheel
python -m build --wheel --outdir dist

Write-Host "Wheel written to $Root\dist — upload to Fabric Environment or copy to Files/lib"