"""Historical Day 5 entry point.

The maintained completion orchestration tests use the current attempt-scoped
PostgreSQL accounting contract.
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
                "tests/test_execute_completion.py",
                "-v",
            ]
        )
    )
