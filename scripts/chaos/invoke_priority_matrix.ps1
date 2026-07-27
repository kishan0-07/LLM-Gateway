$ErrorActionPreference = "Stop"

if (-not $env:GATEWAY_CHAOS_API_KEY) {
    throw "GATEWAY_CHAOS_API_KEY is required"
}

$compose = @(
    "-p", "gateway-day20",
    "--env-file", ".env.production",
    "-f", "docker-compose.production.yml",
    "-f", "docker-compose.chaos.yml"
)

function Wait-Ready {
    for ($attempt = 1; $attempt -le 40; $attempt++) {
        try {
            $response = Invoke-WebRequest `
                -Uri "http://127.0.0.1/ready" `
                -UseBasicParsing `
                -TimeoutSec 3
            if ($response.StatusCode -eq 200) {
                return
            }
        }
        catch {
            Start-Sleep -Seconds 1
        }
    }
    throw "Gateway did not become ready"
}

function Invoke-StateProbe {
    param(
        [Parameter(Mandatory = $true)][string]$TraceId,
        [Parameter(Mandatory = $true)][string]$Mode
    )

    docker compose @compose run --rm chaos-tools `
        /opt/chaos/state_probe.py `
        --trace-id $TraceId `
        --mode $Mode

    if ($LASTEXITCODE -ne 0) {
        throw "State probe failed for $TraceId"
    }
}

Write-Host "1/4 Redis outage: rate limiter must fail closed"
$redisTrace = "day20-redis-$([guid]::NewGuid().ToString('N'))"
try {
    docker compose @compose stop redis
    uv run python scripts/chaos/http_error_probe.py `
        --expected-status 503 `
        --expected-code rate_limiter_unavailable `
        --trace-id $redisTrace
    if ($LASTEXITCODE -ne 0) {
        throw "Redis outage HTTP probe failed"
    }
}
finally {
    docker compose @compose start redis
    Wait-Ready
}
Invoke-StateProbe -TraceId $redisTrace -Mode "rate-limit"

Write-Host "2/4 PostgreSQL outage: request must fail before provider spend"
$postgresTrace = "day20-postgres-$([guid]::NewGuid().ToString('N'))"
try {
    docker compose @compose stop postgres
    uv run python scripts/chaos/http_error_probe.py `
        --expected-status 503 `
        --expected-code database_unavailable `
        --trace-id $postgresTrace
    if ($LASTEXITCODE -ne 0) {
        throw "PostgreSQL outage HTTP probe failed"
    }
}
finally {
    docker compose @compose start postgres
    Wait-Ready
}
Invoke-StateProbe -TraceId $postgresTrace -Mode "database-preflight"

Write-Host "3/4 SSE through Nginx"
uv run python scripts/chaos/sse_probe.py
if ($LASTEXITCODE -ne 0) {
    throw "Nginx SSE probe failed"
}

Write-Host "4/4 Client disconnect and durable finalization"
$disconnectJson = uv run python scripts/chaos/disconnect_client.py |
    ConvertFrom-Json
Invoke-StateProbe -TraceId $disconnectJson.trace_id -Mode "disconnect"

Write-Host "Priority chaos matrix passed"
