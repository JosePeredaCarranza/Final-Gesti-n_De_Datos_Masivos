param(
    [string]$JupyterExecutable = ""
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$notebook = Join-Path $projectRoot "notebooks\gold\modelo_estrella_parquet_powerbi.ipynb"

if (-not (Test-Path -LiteralPath $notebook)) {
    throw "No se encontro el notebook Gold: $notebook"
}

if ([string]::IsNullOrWhiteSpace($JupyterExecutable)) {
    $jupyterCommand = Get-Command jupyter -ErrorAction SilentlyContinue
    if ($null -eq $jupyterCommand) {
        throw "Jupyter no esta disponible en PATH. Indica su ruta con -JupyterExecutable."
    }
    $JupyterExecutable = $jupyterCommand.Source
}

$startedAt = Get-Date
Push-Location $projectRoot
try {
    & $JupyterExecutable nbconvert `
        --to notebook `
        --execute `
        --inplace `
        --ExecutePreprocessor.timeout=-1 `
        $notebook

    if ($LASTEXITCODE -ne 0) {
        throw "La ejecucion de Gold termino con codigo $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}

$duration = (Get-Date) - $startedAt
Write-Host ("Gold finalizo correctamente en {0}." -f $duration)
