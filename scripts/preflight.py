#!/usr/bin/env python3

import asyncio
import shutil
import subprocess
import sys

from sqlalchemy import text

from app.infrastructure.db.session import engine
from app.infrastructure.redis.client import close_redis, get_redis


def check(label, passed, detail=""):
    print(f"{'pass' if passed else 'failed'} {label}{': ' + detail if detail else ''}")
    return passed


async def check_runtime_dependencies() -> list[bool]:
    checks: list[bool] = []
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        checks.append(check("PostgreSQL reachable", True))
    except Exception as exc:
        checks.append(check("PostgreSQL reachable", False, type(exc).__name__))

    try:
        if not await get_redis().ping():
            raise RuntimeError("Redis PING did not return success")
        checks.append(check("Redis reachable", True))
    except Exception as exc:
        checks.append(check("Redis reachable", False, type(exc).__name__))
    finally:
        await close_redis()
        await engine.dispose()
    return checks


def main():
    checks = [
        check(
            "Python >= 3.13", sys.version_info[:2] >= (3, 13), sys.version.split()[0]
        ),
        check("git", shutil.which("git") is not None),
        check("uv", shutil.which("uv") is not None),
        check("docker", shutil.which("docker") is not None),
    ]
    for name, cmd in [
        ("docker compose plugin", ["docker", "compose", "version"]),
        ("Docker daemon running", ["docker", "info"]),
    ]:
        try:
            subprocess.run(cmd, capture_output=True, check=True, timeout=5)
            checks.append(check(name, True))
        except Exception:
            checks.append(check(name, False))
    checks.extend(asyncio.run(check_runtime_dependencies()))
    if not all(checks):
        print("\nFAILED.")
        sys.exit(1)
    print("\nPASSED.")


if __name__ == "__main__":
    main()
