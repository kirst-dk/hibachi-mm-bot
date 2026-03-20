#!/usr/bin/env python3
import hibachi_xyz.types as types

print("Available types in hibachi_xyz.types:")
print("=" * 70)

for attr in dir(types):
    if not attr.startswith('_'):
        obj = getattr(types, attr)
        print(f"  {attr}: {type(obj)}")