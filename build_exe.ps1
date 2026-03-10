#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Builds a standalone Windows executable for the Kafka CLI tool.

.DESCRIPTION
    Uses PyInstaller to bundle kafka_cli.py and all its dependencies into a
    single self-contained kafka-cli.exe.  No Python installation is required
    to run the resulting executable.

    NOTE: pex has a known resolver bug on Windows ("not enough values to
    unpack") that makes it unusable here.  PyInstaller is the standard
    Windows-compatible alternative.

.PARAMETER OutputName
    Base name of the generated executable (no extension).  Defaults to
    "kafka-cli".  The final file will be dist\<OutputName>.exe.

.PARAMETER Clean
    If set, remove build\, dist\, and any leftover .spec file before building.
#>
param(
    [string]$OutputName = "kafka-cli",
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

# --- Locate venv executables --------------------------------------------------
$VenvRoot  = Join-Path $PSScriptRoot "venv"
$PythonExe = Join-Path $VenvRoot "Scripts\python.exe"
$PipExe    = Join-Path $VenvRoot "Scripts\pip.exe"

if (-not (Test-Path $PythonExe)) {
    Write-Error "venv not found at '$VenvRoot'. Create it first: python -m venv venv"
    exit 1
}

# --- Optional clean -----------------------------------------------------------
if ($Clean) {
    foreach ($item in @("build", "dist", "$OutputName.spec")) {
        if (Test-Path $item) {
            Write-Host "==> Removing '$item'..." -ForegroundColor Yellow
            Remove-Item $item -Recurse -Force
        }
    }
}

# --- Install PyInstaller ------------------------------------------------------
Write-Host "==> Installing / upgrading PyInstaller into venv..." -ForegroundColor Cyan
& $PipExe install --quiet --upgrade pyinstaller
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# --- Build executable ---------------------------------------------------------
Write-Host "==> Building '$OutputName.exe'..." -ForegroundColor Cyan

& $PythonExe -m PyInstaller `
    --onefile `
    --name $OutputName `
    --collect-all jsonpath_ng `
    --hidden-import kafka `
    --hidden-import kafka.vendor `
    --hidden-import kafka.vendor.six `
    --hidden-import kafka.vendor.six.moves `
    kafka_cli.py

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Error "PyInstaller build failed (exit code $LASTEXITCODE)."
    exit $LASTEXITCODE
}

# --- Summary ------------------------------------------------------------------
$ExePath = Join-Path "dist" "$OutputName.exe"
$sizeMB  = [math]::Round((Get-Item $ExePath).Length / 1MB, 1)

Write-Host ""
Write-Host "==> Success!  Created: $ExePath  ($sizeMB MB)" -ForegroundColor Green
Write-Host ""
Write-Host "Usage examples:" -ForegroundColor Yellow
Write-Host "  dist\$OutputName --help"
Write-Host "  dist\$OutputName -b localhost:9092 consume -t my-topic"
Write-Host "  dist\$OutputName -b localhost:9092 consume -t my-topic --from-beginning --pretty"
Write-Host "  dist\$OutputName -b localhost:9092 produce -t my-topic -m '{`"hello`":`"world`"}'"
Write-Host ""
Write-Host "Copy dist\$OutputName.exe anywhere - no Python installation needed!" -ForegroundColor Green
