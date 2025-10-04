# Data Lakehouse Platform - Health Check Script#!/usr/bin/env pwsh

Write-Host "`n=== Data Lakehouse Health Check ===" -ForegroundColor Cyan# Data Lakehouse Platform - Health Check Script

Write-Host ""# This script verifies all services are running correctly



$failCount = 0Write-Host "`n╔════════════════════════════════════════════════════════╗" -ForegroundColor Cyan

Write-Host "║  Data Lakehouse Platform - Health Check               ║" -ForegroundColor Cyan

# 1. Container StatusWrite-Host "╚════════════════════════════════════════════════════════╝`n" -ForegroundColor Cyan

Write-Host "1. Container Status:" -ForegroundColor Yellow

docker ps --format "table {{.Names}}\t{{.Status}}" | Where-Object { $_ -match '(minio|postgres|iceberg|mlflow|prefect|trino|NAMES)' }$script:failureCount = 0

Write-Host ""

# ============================================================================

# 2. Service Endpoints# 1. Container Status Check

Write-Host "2. Service Endpoints:" -ForegroundColor Yellow# ============================================================================

$endpoints = @{Write-Host "1. Checking Container Status..." -ForegroundColor Yellow

    'MinIO'    = 'http://localhost:9000/minio/health/ready'Write-Host ("=" * 80) -ForegroundColor DarkGray

    'Iceberg'  = 'http://localhost:8181/v1/config'

    'MLflow'   = 'http://localhost:5000/'try {

    'Prefect'  = 'http://localhost:4200/api/health'    $containers = docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | 

}                  Where-Object { $_ -match '(minio|postgres|iceberg|mlflow|prefect|trino|NAMES)' }

    

foreach ($ep in $endpoints.GetEnumerator()) {    if ($containers) {

    try {        $containers | ForEach-Object { Write-Host $_ }

        $r = Invoke-WebRequest -Uri $ep.Value -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop        Write-Host "✓ Container status retrieved successfully`n" -ForegroundColor Green

        Write-Host "  $($ep.Key): OK ($($r.StatusCode))" -ForegroundColor Green    } else {

    } catch {        Write-Host "✗ No lakehouse containers found running`n" -ForegroundColor Red

        Write-Host "  $($ep.Key): FAILED" -ForegroundColor Red        $script:failureCount++

        $failCount++    }

    }} catch {

}    Write-Host "✗ Failed to get container status: $($_.Exception.Message)`n" -ForegroundColor Red

    $script:failureCount++

Write-Host "  PostgreSQL:" -NoNewline}

try {

    docker exec postgres pg_isready -U pvlakehouse >$null 2>&1# ============================================================================

    if ($LASTEXITCODE -eq 0) {# 2. Service Endpoint Tests

        Write-Host " OK" -ForegroundColor Green# ============================================================================

    } else {Write-Host "2. Testing Service Endpoints..." -ForegroundColor Yellow

        Write-Host " FAILED" -ForegroundColor RedWrite-Host ("=" * 80) -ForegroundColor DarkGray

        $failCount++

    }$endpoints = [ordered]@{

} catch {    'MinIO API'         = 'http://localhost:9000/minio/health/ready'

    Write-Host " FAILED" -ForegroundColor Red    'MinIO Console'     = 'http://localhost:9001/minio/health/ready'

    $failCount++    'PostgreSQL'        = 'localhost:5432'  # Special case - will test via docker

}    'Iceberg REST'      = 'http://localhost:8181/v1/config'

    'MLflow'            = 'http://localhost:5000/'

Write-Host ""    'Prefect API'       = 'http://localhost:4200/api/health'

    'Trino'             = 'http://localhost:8081/v1/info'

# 3. Databases}

Write-Host "3. PostgreSQL Databases:" -ForegroundColor Yellow

docker exec postgres psql -U pvlakehouse -d postgres -c "\l" 2>$null | Select-String -Pattern '(iceberg|mlflow|prefect)'foreach ($endpoint in $endpoints.GetEnumerator()) {

Write-Host ""    $name = $endpoint.Key

    $url = $endpoint.Value

# 4. S3 Buckets    

Write-Host "4. MinIO Buckets:" -ForegroundColor Yellow    Write-Host "  Testing $name... " -NoNewline

docker run --rm --network docker_data-net --entrypoint sh minio/mc:latest -c "mc alias set local http://minio:9000 pvlakehouse pvlakehouse >/dev/null 2>&1; mc ls local" 2>&1 | Select-String '\['    

Write-Host ""    if ($name -eq 'PostgreSQL') {

        # Special handling for PostgreSQL

# Summary        try {

Write-Host "=== Health Check Complete ===" -ForegroundColor Cyan            $result = docker exec postgres pg_isready -U pvlakehouse 2>&1

if ($failCount -eq 0) {            if ($LASTEXITCODE -eq 0) {

    Write-Host "All services are healthy!`n" -ForegroundColor Green                Write-Host "✓ OK" -ForegroundColor Green

    exit 0            } else {

} else {                Write-Host "✗ FAILED" -ForegroundColor Red

    Write-Host "$failCount issue(s) detected`n" -ForegroundColor Red                $script:failureCount++

    exit 1            }

}        } catch {

            Write-Host "✗ FAILED" -ForegroundColor Red
            $script:failureCount++
        }
    } else {
        # HTTP endpoint testing
        try {
            $response = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
            Write-Host "✓ OK (HTTP $($response.StatusCode))" -ForegroundColor Green
        } catch {
            Write-Host "✗ FAILED" -ForegroundColor Red
            $script:failureCount++
        }
    }
}
Write-Host ""

# ============================================================================
# 3. PostgreSQL Database Check
# ============================================================================
Write-Host "3. Checking PostgreSQL Databases..." -ForegroundColor Yellow
Write-Host ("=" * 80) -ForegroundColor DarkGray

try {
    $databases = docker exec postgres psql -U pvlakehouse -d postgres -c "\l" 2>&1 | 
                 Select-String -Pattern '(iceberg|mlflow|prefect)'
    
    if ($databases) {
        Write-Host "  Found application databases:" -ForegroundColor Cyan
        $databases | ForEach-Object { Write-Host "    $_" -ForegroundColor White }
        Write-Host "✓ All required databases exist`n" -ForegroundColor Green
    } else {
        Write-Host "✗ Required databases not found`n" -ForegroundColor Red
        $script:failureCount++
    }
} catch {
    Write-Host "✗ Failed to check databases: $($_.Exception.Message)`n" -ForegroundColor Red
    $script:failureCount++
}

# ============================================================================
# 4. MinIO S3 Buckets Check
# ============================================================================
Write-Host "4. Checking MinIO S3 Buckets..." -ForegroundColor Yellow
Write-Host ("=" * 80) -ForegroundColor DarkGray

try {
    $buckets = docker run --rm --network docker_data-net --entrypoint sh minio/mc:latest -c "mc alias set local http://minio:9000 pvlakehouse pvlakehouse > /dev/null 2>&1; mc ls local" 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  Available buckets:" -ForegroundColor Cyan
        $buckets | Where-Object { $_ -match '\[' } | ForEach-Object { 
            Write-Host "    $_" -ForegroundColor White 
        }
        
        if ($buckets -match 'lakehouse' -and $buckets -match 'mlflow') {
            Write-Host "✓ All required buckets exist`n" -ForegroundColor Green
        } else {
            Write-Host "✗ Some required buckets are missing`n" -ForegroundColor Red
            $script:failureCount++
        }
    } else {
        Write-Host "✗ Failed to list buckets`n" -ForegroundColor Red
        $script:failureCount++
    }
} catch {
    Write-Host "✗ Failed to check buckets: $($_.Exception.Message)`n" -ForegroundColor Red
    $script:failureCount++
}

# ============================================================================
# 5. Docker Resource Usage
# ============================================================================
Write-Host "5. Docker Resource Usage..." -ForegroundColor Yellow
Write-Host ("=" * 80) -ForegroundColor DarkGray

try {
    $stats = docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" | 
             Where-Object { $_ -match '(minio|postgres|iceberg|mlflow|prefect|trino|NAME)' }
    
    if ($stats) {
        $stats | ForEach-Object { Write-Host $_ }
        Write-Host ""
    }
} catch {
    Write-Host "Could not retrieve resource stats`n" -ForegroundColor Yellow
}

# ============================================================================
# Summary
# ============================================================================
Write-Host ("=" * 80) -ForegroundColor DarkGray
if ($script:failureCount -eq 0) {
    Write-Host "✓ Health Check Passed - All services are healthy!" -ForegroundColor Green
    Write-Host "`nAll systems operational. Ready for use.`n" -ForegroundColor Cyan
    exit 0
} else {
    Write-Host "✗ Health Check Failed - $($script:failureCount) issue(s) detected" -ForegroundColor Red
    Write-Host "`nPlease check the logs for failed services:" -ForegroundColor Yellow
    Write-Host '  docker compose logs [service-name]' -ForegroundColor Yellow
    Write-Host ""
    exit 1
}