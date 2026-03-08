# 1. Check ClickHouse Port
Write-Host ">>> API: Checking ClickHouse (8123) port..." -ForegroundColor Cyan
$maxRetries = 30
$retryCount = 0
$portOpen = $false
while ($retryCount -lt $maxRetries) {
    if (Test-NetConnection -ComputerName localhost -Port 8123 -InformationLevel Quiet) {
        $portOpen = $true
        break
    }
    Write-Host "Wrapper: Waiting for ClickHouse... ($($retryCount+1)/$maxRetries)" -ForegroundColor Yellow
    Start-Sleep -Seconds 2
    $retryCount++
}
if (-not $portOpen) {
    Write-Host "Error: ClickHouse did not respond on port 8123." -ForegroundColor Red
    Write-Host "Please ensure Docker is running: cd GlobalInsight-Env; docker-compose up -d" -ForegroundColor Gray
    exit 1
}
Write-Host "ClickHouse is READY!" -ForegroundColor Green
# 2. Initialize Database (Idempotent)
Write-Host ">>> API: Initializing database schema..." -ForegroundColor Cyan
try {
    # Define SQL commands clearly
    $createDbSql = "CREATE DATABASE IF NOT EXISTS globalinsight"
    $createTableSql = "CREATE TABLE IF NOT EXISTS globalinsight.behavior_olap (user_id String, event_type String, item_id String, ts UInt64, age UInt8, level String, session_id String) ENGINE = MergeTree() ORDER BY ts"
    # Execute SQL inside container
    Write-Host "Creating Database..."
    docker exec clickhouse clickhouse-client --query "$createDbSql"
    Write-Host "Creating Table..."
    docker exec clickhouse clickhouse-client --query "$createTableSql"
    Write-Host "Schema initialized." -ForegroundColor Green
} catch {
    Write-Host "Warning during SQL initialization. Please check if ClickHouse is accessible." -ForegroundColor Yellow
    Write-Host $_
}
# 3. Run Java Application
Write-Host ">>> API: Starting ClickHouseSinkApp..." -ForegroundColor Cyan
Set-Location "$PSScriptRoot\Code"
# Using cmd /c to ensure proper argument parsing for Maven
cmd /c "mvn exec:java -Dexec.mainClass=ClickHouseSinkApp"

