# Shared helpers for the demo scripts (dot-source this file).
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
Set-Location (Split-Path -Parent $PSScriptRoot)

$Topic = if ($env:KAFKA_TOPIC) { $env:KAFKA_TOPIC } else { 'products' }
$ConsumerGroup = 'products-mongo-sync'

function Write-Step([string]$Title) {
    Write-Host ''
    Write-Host ('=' * 78) -ForegroundColor DarkCyan
    Write-Host "  $Title" -ForegroundColor Cyan
    Write-Host ('=' * 78) -ForegroundColor DarkCyan
}

function Invoke-Native([scriptblock]$Command) {
    $ErrorActionPreference = 'Continue'
    & $Command 2>&1 | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) { "$($_.Exception.Message)" } else { "$_" }
    }
}

function Invoke-Sql([string]$Query, [switch]$Scalar) {
    $flags = if ($Scalar) { '-tAq' } else { '-q' }
    Invoke-Native { $Query | docker compose exec -T postgres sh -c "psql -v ON_ERROR_STOP=1 $flags -U `$POSTGRES_USER -d `$POSTGRES_DB" }
    if ($LASTEXITCODE -ne 0) { throw "SQL failed: $Query" }
}

function Invoke-Mongo([string]$Script) {
    # Windows PowerShell 5.1 mangles embedded double quotes in native arguments,
    # so the script travels via stdin into a temp file instead of --eval "...".
    $shell = 'f=$(mktemp); cat > $f; mongosh --quiet -u $MONGO_INITDB_ROOT_USERNAME -p $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin $MONGO_APP_DATABASE --file $f; rc=$?; rm -f $f; exit $rc'
    Invoke-Native { $Script | docker compose exec -T mongo sh -c $shell }
    if ($LASTEXITCODE -ne 0) { throw 'mongosh failed' }
}

function Invoke-KafkaTool([string]$Tool, [string[]]$Arguments) {
    Invoke-Native { docker compose exec -T kafka "/opt/kafka/bin/$Tool" --bootstrap-server kafka:29092 @Arguments } |
        Where-Object { $_ -notmatch 'KIP-848|consumer_rebalance_protocol|Processed a total of' }
}

function Show-ServiceLogs([string]$Service, [string]$Since = '30s', [string]$Pattern = '.') {
    Invoke-Native { docker compose logs --no-log-prefix --since $Since $Service } | Select-String -Pattern $Pattern
}

function Show-MongoProduct([int]$Id) {
    Invoke-Mongo "printjson(db.products.findOne({ _id: $Id }))"
}

function Wait-ForMongoVersion([int]$Id, [string]$Field, [string]$Expected, [int]$TimeoutSeconds = 30) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $value = Invoke-Mongo "const d = db.products.findOne({ _id: $Id }); print(d ? String(d.$Field) : '')"
        if ("$value".Trim() -eq $Expected) { return $true }
        Start-Sleep -Milliseconds 500
    }
    Write-Warning "MongoDB document $Id did not reach $Field=$Expected within $TimeoutSeconds s"
    return $false
}
