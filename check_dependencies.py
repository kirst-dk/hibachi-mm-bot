#!/usr/bin/env python3
import sys


def check_python():
    major, minor = sys.version_info[:2]
    if major < 3 or (major == 3 and minor < 7):
        print(f"FAIL: Python {major}.{minor} (need 3.7+)")
        return False
    print(f"PASS: Python {major}.{minor}")
    return True


def check_package(name, min_ver=None):
    try:
        from importlib.metadata import version
        ver = version(name)
        print(f"PASS: {name} {ver}")
        return True
    except:
        print(f"FAIL: {name} not installed")
        return False


def check_import(module, pkg=None):
    try:
        __import__(module)
        print(f"PASS: {module} imports OK")
        return True
    except ImportError:
        print(f"FAIL: {module} import failed")
        if pkg:
            print(f"      Install: pip install {pkg}")
        return False


def main():
    print("=" * 70)
    print("DEPENDENCY CHECK")
    print("=" * 70)

    all_ok = True

    print("\n1. Python Version")
    print("-" * 70)
    all_ok &= check_python()

    print("\n2. Required Packages")
    print("-" * 70)
    all_ok &= check_package("hibachi-xyz", "0.1.14")
    all_ok &= check_package("python-dotenv", "1.0.0")
    all_ok &= check_package("requests", "2.31.0")
    all_ok &= check_package("certifi")

    print("\n3. Import Tests")
    print("-" * 70)
    all_ok &= check_import("hibachi_xyz", "hibachi-xyz")
    all_ok &= check_import("dotenv", "python-dotenv")
    all_ok &= check_import("requests")

    print("\n" + "=" * 70)
    if all_ok:
        print("ALL DEPENDENCIES OK")
        print("\nNext: cp .env.example .env")
        print("Then: edit .env with your API keys")
        return 0
    else:
        print("MISSING DEPENDENCIES")
        print("\nRun: pip install -r requirements.txt")
        return 1


if __name__ == "__main__":
    sys.exit(main())