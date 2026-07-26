"""Historical Day 4 entry point.

The original script exercised the retired Redis-authoritative budget adapter.
The maintained PostgreSQL concurrency and idempotency coverage now lives in
``tests/test_budget.py``.
"""

import subprocess
import sys


if __name__ == "__main__":
    raise SystemExit(
        subprocess.call(
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/test_budget.py",
                "-v",
                "-m",
                "integration",
            ]
        )
    )
