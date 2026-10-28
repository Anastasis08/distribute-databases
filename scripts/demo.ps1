<#
.SYNOPSIS
  Full demonstration: INSERT and UPDATE in PostgreSQL -> Producer -> Kafka -> Consumer -> MongoDB,
  plus a re-delivery of the same message to prove there are no duplicates.
.EXAMPLE
  docker compose up -d --build ; .\scripts\demo.ps1
#>
. "$PSScriptRoot\common.ps1"

Write-Step '0. Running services'
docker compose ps --format 'table {{.Service}}\t{{.Status}}'

Write-Step "0. Kafka topic '$Topic'"
Invoke-KafkaTool 'kafka-topics.sh' @('--describe', '--topic', $Topic)

Write-Step '0. SQL: current products'
Invoke-Sql 'SELECT id, sku, name, price, quantity, updated_at FROM products ORDER BY id;'

# ---------------------------------------------------------------- Operation 1: INSERT
$sku = 'CAM-' + (Get-Date -Format 'HHmmss')
Write-Step "1. INSERT into PostgreSQL (sku=$sku)"
$id = [int](Invoke-Sql "INSERT INTO products (sku, name, category, price, quantity) VALUES ('$sku', 'Webcam Logitech Brio 4K', 'Video', 7499.00, 10) RETURNING id;" -Scalar | Select-Object -First 1)
Write-Host "New product id = $id" -ForegroundColor Green
Invoke-Sql "SELECT id, sku, name, price, quantity, created_at, updated_at FROM products WHERE id = $id;"

Write-Host 'Waiting for the change to travel SQL -> Kafka -> MongoDB ...'
[void](Wait-ForMongoVersion -Id $id -Field 'price' -Expected '7499.00')

Write-Step '1. Producer log (change detected and published)'
Show-ServiceLogs 'producer' '1m' "id=$id\b|Detected|Checkpoint"
Write-Step '1. Consumer log (message consumed and stored)'
Show-ServiceLogs 'consumer' '1m' "id=$id\b"
Write-Step '1. MongoDB document'
Show-MongoProduct $id

# ---------------------------------------------------------------- Operation 2: UPDATE
Write-Step '2. UPDATE in PostgreSQL (price 7499.00 -> 6999.00, quantity 10 -> 7)'
Invoke-Sql "UPDATE products SET price = 6999.00, quantity = 7 WHERE id = $id;"
Invoke-Sql "SELECT id, sku, price, quantity, created_at, updated_at FROM products WHERE id = $id;"
Write-Host 'updated_at was refreshed by the trigger. Waiting for synchronization ...'
[void](Wait-ForMongoVersion -Id $id -Field 'price' -Expected '6999.00')

Write-Step '2. Producer / Consumer logs'
Show-ServiceLogs 'producer' '30s' "id=$id\b"
Show-ServiceLogs 'consumer' '30s' "id=$id\b"
Write-Step '2. MongoDB document after update'
Show-MongoProduct $id

# ---------------------------------------------------------------- Kafka messages
Write-Step "3. Messages with key=$id in Kafka topic '$Topic'"
Invoke-KafkaTool 'kafka-console-consumer.sh' @(
    '--topic', $Topic, '--from-beginning', '--timeout-ms', '6000',
    '--property', 'print.key=true', '--property', 'print.partition=true',
    '--property', 'print.offset=true', '--property', 'key.separator= | '
) | Select-String -Pattern "(^|\s)$id \| \{"

# ---------------------------------------------------------------- Duplicate delivery
Write-Step "4. Re-deliver the same record (id=$id) through Kafka"
Invoke-Native { docker compose exec -T producer product-producer republish --id $id }
Start-Sleep -Seconds 4
Show-ServiceLogs 'consumer' '15s' "id=$id\b"
Write-Host 'Number of MongoDB documents with this _id:' -ForegroundColor Green
Invoke-Mongo "print(db.products.countDocuments({ _id: $id }))"

# ---------------------------------------------------------------- Reconciliation
Write-Step '5. Reconciliation SQL vs MongoDB'
$sqlCount = (Invoke-Sql 'SELECT count(*) FROM products;' -Scalar | Select-Object -First 1).Trim()
$mongoCount = ("$(Invoke-Mongo 'print(db.products.countDocuments({}))')").Trim()
Write-Host "PostgreSQL rows: $sqlCount   MongoDB documents: $mongoCount" -ForegroundColor Green
Write-Host 'Detailed field-by-field check: uv run python scripts/verify_sync.py'
