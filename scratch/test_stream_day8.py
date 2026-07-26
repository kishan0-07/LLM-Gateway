"""Historical Day 8 entry point.

The maintained stream tests cover durable completion, timeout, failure,
cancellation, and shutdown-drain behavior.
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
                "tests/test_stream_completion.py",
                "-v",
            ]
        )
    )
