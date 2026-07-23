import importlib
import pkgutil

PACKAGES = [
    "app.api",
    "app.application",
    "app.domain",
    "app.infrastructure",
    "app.workers",
]


def check_package(pkg_name: str):
    pkg = importlib.import_module(pkg_name)
    errors = []
    for _, name, _ in pkgutil.walk_packages(pkg.__path__, prefix=pkg_name + "."):
        try:
            importlib.import_module(name)
        except Exception as e:
            errors.append((name, repr(e)))
    return errors


def main():
    all_errors = []
    for pkg in PACKAGES:
        errors = check_package(pkg)
        print(f"{pkg}: {'OK' if not errors else f'{len(errors)} FAILED'}")
        all_errors.extend(errors)
    for name, err in all_errors:
        print(f"  {name}: {err}")
    if all_errors:
        raise SystemExit(f"{len(all_errors)} import failures")
    print("ALL PACKAGES IMPORT CLEAN")


if __name__ == "__main__":
    main()
