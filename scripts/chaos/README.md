# Day 20 priority-chaos tools

These scripts exercise GatewayLLM's production-shaped Docker and Nginx path.
They deliberately stop Redis and PostgreSQL, make real provider calls, and
inspect durable PostgreSQL state after recovery.

## Safety rules

- Run only against the isolated `gateway-day20` Compose project.
- Confirm no unrelated local service uses host port 80.
- Real SSE and disconnect probes make low-volume provider calls and incur a
  small cost.
- Keep `.env.production` and `GATEWAY_CHAOS_API_KEY` local. Never commit them or
  copy their values into evidence.
- The chaos overlay disables Langfuse export so external telemetry availability
  cannot affect deterministic failure-policy evidence.
- The orchestrator restores Redis and PostgreSQL in `finally` blocks. Confirm
  `GET /ready` succeeds after every failure.
- Clean only the tenant created by `tenant_tool.py`. Do not delete database
  volumes to remove chaos data.
- If the stack must be removed, target the Day 20 project explicitly:

  ```powershell
  docker compose -p gateway-day20 `
      --env-file .env.production `
      -f docker-compose.production.yml `
      -f docker-compose.chaos.yml down
  ```

- Locust is secondary work. Do not run it until the priority matrix passes
  twice.

## Required local environment

```powershell
$env:GATEWAY_IMAGE = "llm-gateway:day20"
$env:GATEWAY_CHAOS_API_KEY = "<key returned by tenant_tool.py>"
$env:GATEWAY_CHAOS_MODEL = "openai/gpt-oss-20b"  # optional
```

Never echo or persist `GATEWAY_CHAOS_API_KEY`.

## Execution

Build and start the isolated stack, seed a chaos tenant, then run:

```powershell
powershell -ExecutionPolicy Bypass `
    -File scripts/chaos/invoke_priority_matrix.ps1
```

Run the matrix twice with a freshly seeded tenant and fresh trace IDs. Record
only redacted results in `docs/chaos/day20-results.md`.
