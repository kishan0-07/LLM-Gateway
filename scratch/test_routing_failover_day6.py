"""Historical Day 6 entry point.

The maintained unit and HTTP smoke tests now verify provider failure, invalid
output, circuit health, and billed failover attempts together.
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
                "tests/test_priority_smoke.py",
                "-v",
                "-k",
                "fallback or all_candidates_failed",
            ]
        )
    )
