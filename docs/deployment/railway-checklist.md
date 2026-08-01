# Railway Deployment Checklist

Use this checklist for the first deployment and every material production
configuration change. Never copy real credentials into this file, screenshots,
terminal recordings, or Git history.

## 0. Railway dashboard sequence

1. In Railway, choose **New Project → Deploy from GitHub repo**, select this
   repository, and create the GatewayLLM service.
2. On the same project canvas, choose **+ New → Database → PostgreSQL**, then
   repeat for **Redis**. Name the services `Postgres` and `Redis`, or use the
   dashboard's variable-reference autocomplete instead of typing service names.
3. Open each service's **Settings → Region** and place all three services in the
   same region.
4. Open **GatewayLLM → Variables**. Add the variables in section 2, using
   reference variables for database/Redis URLs and sealed values for secrets.
   Review and deploy the staged variable changes.
5. Open **GatewayLLM → Settings → Source**. Select the shipping branch, keep
   GitHub autodeploy enabled, and turn on **Wait for CI**.
6. Open **GatewayLLM → Settings → Networking → Public Networking** and choose
   **Generate Domain**.
7. Inspect the PostgreSQL and Redis networking settings. Do not generate a
   domain or TCP proxy for either database.
8. Configure a Railway project usage limit/alerts before provider-backed smoke
   or load work.
9. Deploy the exact GitHub commit whose CI check is green. Confirm the
   pre-deploy migration succeeds before the application deployment becomes
   active.

Current Railway references:

- [GitHub autodeploy and Wait for CI](https://docs.railway.com/deployments/github-autodeploys)
- [Reference and sealed variables](https://docs.railway.com/variables)
- [Private networking](https://docs.railway.com/private-networking)
- [Generate a public domain](https://docs.railway.com/networking/public-networking)
- [Pre-deploy commands](https://docs.railway.com/deployments/pre-deploy-command)

### Observed production snapshot — August 1, 2026 (redeploy update at 17:33 UTC)

| Item | Observed value | Result |
|---|---|---|
| Public gateway | <https://llm-gateway-7.up.railway.app> | Active |
| Original authenticated-smoke deployment | `8fc4e474-ef4a-4feb-83fc-87021def77be` | Historical evidence retained |
| Original authenticated-smoke SHA | `53f8f784b64578b91745849269f186223df2f45b` | Exact app/CI/Railway match at smoke time |
| Current `origin/main` SHA | `7eed53633aea47bf80d3076d1748602e0764d51d` | Pushed |
| Current CI | [run 30690883651](https://github.com/kishan0-07/LLM-Gateway/actions/runs/30690883651) | Passed |
| Post-rotation redeploy | Current main redeployed; new Railway deployment ID not recorded in this repository | Operator reported |
| Region / replicas | US West (`sfo`) / 1 | Verified |
| Resource ceiling | 2 vCPU / 1 GB RAM | Verified |
| Pre-deploy command | `alembic upgrade head` | Configured |
| Live Alembic revision | `e7f4a2c91b60 (head)` | Verified |
| Health and readiness | `/health` 200; `/ready` 200 | Passed |
| Runtime PostgreSQL host | `postgres.railway.internal` | Private path in use |
| Runtime Redis host | `redis.railway.internal` | Private path in use |
| PostgreSQL public TCP proxy | Removed | Passed |
| Redis public TCP proxy | Removed | Passed |
| Live accounting probes | Two requests; both settled, zero hold, ledger parity and cost equality | Passed |
| Langfuse | Two live traces matched PostgreSQL/log evidence; sanitized excerpts and allowlisted metadata only | Passed |
| Post-redeploy public recovery | `/health`, `/ready`, and `/openapi.json` returned HTTP 200 at 2026-08-01 17:33 UTC | Passed |
| In-flight stream termination/drain | No stream was observed across termination | Pending |
| Temporary smoke-key rotation | Rotation completed; credentials not reused during this documentation audit | Operator reported |
| Old-key rejection / new-key auth / one-active-key proof | No secret-bearing probe was run during this documentation audit | Pending |

The complete redacted results are in
[`docs/evidence/final-smoke.md`](../evidence/final-smoke.md). Do not interpret
the passing smoke as a capacity or SLO result.

> [!NOTE]
> The application reaches both dependencies through Railway private DNS. The
> unnecessary PostgreSQL and Redis public TCP proxies were removed on August 1,
> 2026, and `/health` plus `/ready` remained green after each removal.

## 1. Project topology

| Service | Source | Exposure |
|---|---|---|
| GatewayLLM | Repository `Dockerfile` | Public HTTPS domain |
| PostgreSQL | Railway PostgreSQL template | Private network only |
| Redis | Railway Redis template | Private network only |

- [ ] All three services use the same Railway region.
- [x] PostgreSQL has no public domain or public TCP proxy.
- [x] Redis has no public domain or public TCP proxy.
- [ ] Only GatewayLLM has a generated public domain.

## 2. Required GatewayLLM variables

| Variable | Production value |
|---|---|
| `DATABASE_URL` | `${{Postgres.DATABASE_URL}}` |
| `REDIS_URL` | `${{Redis.REDIS_URL}}` |
| `ENVIRONMENT` | `production` |
| `GROQ_API_KEY` | Sealed secret, or omit if OpenAI is configured |
| `OPENAI_API_KEY` | Sealed secret, or omit if Groq is configured |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` |
| `RATE_LIMIT_TENANT_REQUESTS` | `120` |
| `RATE_LIMIT_API_KEY_REQUESTS` | `60` |
| `RATE_LIMIT_REDIS_FAILURE_MODE` | `fail_closed` |
| `RESERVATION_RECONCILE_INTERVAL_SECONDS` | `60` |
| `SHUTDOWN_GRACE_SECONDS` | `15` |
| `UVICORN_GRACEFUL_SHUTDOWN_SECONDS` | `50` |
| `LANGFUSE_ENABLED` | `true` or `false` |
| `LANGFUSE_PUBLIC_KEY` | Sealed secret when Langfuse is enabled |
| `LANGFUSE_SECRET_KEY` | Sealed secret when Langfuse is enabled |
| `LANGFUSE_BASE_URL` | Correct region-specific Langfuse URL |

At least one provider API key is mandatory. Configure both only when both
providers should receive traffic.

Do not define `PORT`; Railway injects it. Use the private `DATABASE_URL`, never
`DATABASE_PUBLIC_URL`.

## 3. Deployment policy

The tracked [`railway.json`](../../railway.json) is the source of truth for
these settings:

- [ ] Dockerfile builder with root `Dockerfile`.
- [ ] Pre-deploy command: `alembic upgrade head`.
- [ ] Readiness health check: `/ready`.
- [ ] Health-check timeout: 300 seconds.
- [ ] Restart policy: `ON_FAILURE`, maximum five retries.
- [ ] Draining time: 60 seconds.
- [ ] Uvicorn graceful shutdown remains below the drain window.

In the Railway dashboard:

- [ ] GitHub autodeploy targets the intended shipping branch.
- [ ] “Wait for CI” is enabled before deployment.
- [ ] A project spending limit and usage alerts are configured.
- [ ] The deployment details show settings sourced from `railway.json`.

A failed pre-deploy migration must block activation. Never move migrations back
into the application `CMD`; multiple app replicas must not race migrations.

## 4. First deploy and operator bootstrap

1. Confirm the pre-deploy migration exits successfully.
2. Confirm the deployment becomes healthy at `/ready`.
3. Install/login to the Railway CLI, link the project/service, and use
   `railway ssh`. Unlike `railway shell`, SSH executes inside the deployed
   container and can reach the private service network.
4. Create the first tenant and API key inside the deployed GatewayLLM
   container:

   ```powershell
   railway ssh -s GatewayLLM -- python -m app.cli.create_api_key `
     --tenant-name ship-demo `
     --monthly-limit-usd 5.00
   ```

5. Save the displayed raw key in a password manager. It is intentionally shown
   only once and is never stored by GatewayLLM.
6. Use the key to verify `/whoami`, one non-stream completion, one stream
   completion, `/stats`, and `/stats/me`.

Railway CLI reference:
[connect to a deployed container with `railway ssh`](https://docs.railway.com/cli/ssh).

## 5. Rotation

Record the current active prefix through an authorized database/operator review,
then run:

```bash
python -m app.cli.create_api_key \
  --tenant-name ship-demo \
  --monthly-limit-usd 5.00 \
  --rotate \
  --expected-active-prefix sk-gw-abc123
```

- [x] The guarded rotation command was completed by the operator.
- [ ] Capture that the old key is rejected after rotation.
- [ ] Capture that the new key authenticates successfully.
- [ ] Capture that exactly one active key remains.
- [ ] No raw key appears in logs, Langfuse, screenshots, or committed files.

## 6. Post-deploy proof

- [x] `/health` returns HTTP 200 after the redeploy.
- [x] `/ready` returns HTTP 200 after the redeploy and therefore proves PostgreSQL and Redis
      connectivity.
- [ ] The live smoke matrix passes.
- [ ] All-provider failure returns the standard 503 error contract.
- [ ] Redis failure during budget authorization fails closed.
- [ ] Structured logs contain trace IDs but no prompts, API keys, or provider
      exception text.
- [ ] Langfuse observations contain only sanitized excerpts and allowlisted
      metadata.
- [ ] An in-flight stream receives SIGTERM and finalizes within the 50-second
      Uvicorn budget and 60-second Railway drain window.
- [x] The replacement instance became publicly healthy/ready after the
      controlled redeploy; `railway.json` still configures migrations as the
      pre-deploy command.

### Live smoke commands

Run these from a local PowerShell after saving the one-time gateway key:

```powershell
$env:GATEWAY_BASE_URL = "https://YOUR-GATEWAY-DOMAIN"
$env:GATEWAY_API_KEY = Read-Host "Paste the temporary smoke-test key"
$env:GATEWAY_MODEL = "openai/gpt-oss-20b"

Invoke-RestMethod "$env:GATEWAY_BASE_URL/health"
Invoke-RestMethod "$env:GATEWAY_BASE_URL/ready"
Invoke-RestMethod "$env:GATEWAY_BASE_URL/whoami" `
  -Headers @{"X-API-Key" = $env:GATEWAY_API_KEY}

$before = Invoke-RestMethod "$env:GATEWAY_BASE_URL/stats/me" `
  -Headers @{"X-API-Key" = $env:GATEWAY_API_KEY}

uv run python scripts/demo/concurrent_requests.py

$after = Invoke-RestMethod "$env:GATEWAY_BASE_URL/stats/me" `
  -Headers @{"X-API-Key" = $env:GATEWAY_API_KEY}

$before | ConvertTo-Json -Depth 8
$after | ConvertTo-Json -Depth 8
```

For one selected demo trace, inspect authoritative state inside the deployed
container:

```powershell
railway ssh -s GatewayLLM -- python scripts/chaos/state_probe.py `
  --trace-id TRACE_FROM_DEMO `
  --mode inspect `
  --wait-seconds 20
```

Record the deployment SHA from Railway, the green GitHub CI SHA, safe trace
IDs, stats delta, and probe result in
[`docs/evidence/final-smoke.md`](../evidence/final-smoke.md). Do not copy the
key or raw provider content.

## 7. Rollback and incident notes

- [ ] Record the deployed Git SHA and Alembic revision.
- [ ] Keep the previous healthy deployment available for Railway rollback.
- [ ] Never downgrade the database blindly; review migration compatibility
      before rolling application code backward.
- [ ] If readiness fails, inspect dependency connectivity and migration status
      before restarting repeatedly.
- [ ] If key state is ambiguous (zero or multiple active keys), stop and repair
      it explicitly; do not bypass the CLI safety checks.
