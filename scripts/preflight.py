#!/usr/bin/env python3

import shutil
import subprocess
import sys


def check(label, passed, detail=""):
    print(f"{'pass' if passed else 'failed'} {label}{': ' + detail if detail else ''}")
    return passed


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
    if not all(checks):
        print("\nFAILED.")
        sys.exit(1)
    print("\nPASSED.")


if __name__ == "__main__":
    main()
