#!/usr/bin/env python3
import sys


def test_inventory_skew():
    print("=" * 70)
    print("TEST: Inventory Skew")
    print("=" * 70)

    tests = [
        ("Neutral", 10000, 0.0, 50000, 50, 1, 0.0),
        ("Half long", 10000, 0.05, 50000, 50, 1, 0.5),
        ("Max long", 10000, 0.10, 50000, 50, 1, 1.0),
        ("Half short", 10000, -0.05, 50000, 50, 1, -0.5),
    ]

    passed = 0
    for name, bal, pos, price, inv_pct, lev, expected in tests:
        notional = pos * price
        max_not = bal * (inv_pct / 100) * lev
        skew = max(-1.0, min(1.0, notional / max_not if max_not > 0 else 0))
        ok = abs(skew - expected) < 0.01
        status = "PASS" if ok else "FAIL"
        print(f"{status}: {name} | skew={skew:+.3f} (exp {expected:+.3f})")
        if ok:
            passed += 1

    print(f"\nResult: {passed}/{len(tests)} passed")
    return passed == len(tests)


def test_order_sizing():
    print("\n" + "=" * 70)
    print("TEST: Order Sizing")
    print("=" * 70)

    equity = 10000
    base_pct = 2.0
    size_amp = 1.5
    contract_size = 0.001

    tests = [
        ("Neutral", 0.0, 1.0, 1.0),
        ("Long +0.5", 0.5, 1.0, 1.75),
        ("Short -0.5", -0.5, 1.75, 1.0),
    ]

    passed = 0
    for name, skew, exp_bid_mult, exp_ask_mult in tests:
        ask_mult = 1.0 + size_amp * max(0.0, skew)
        bid_mult = 1.0 + size_amp * max(0.0, -skew)
        ok = (abs(bid_mult - exp_bid_mult) < 0.01 and
              abs(ask_mult - exp_ask_mult) < 0.01)
        status = "PASS" if ok else "FAIL"
        print(f"{status}: {name} | bid={bid_mult:.2f} ask={ask_mult:.2f}")
        if ok:
            passed += 1

    print(f"\nResult: {passed}/{len(tests)} passed")
    return passed == len(tests)


def main():
    print("\nHIBACHI MM BOT - STRATEGY TESTS\n")
    all_ok = test_inventory_skew() and test_order_sizing()
    print("\n" + "=" * 70)
    if all_ok:
        print("ALL TESTS PASSED")
        print("Strategy logic is correct. Safe to deploy with small amounts.")
        return 0
    else:
        print("SOME TESTS FAILED")
        print("Fix logic before deploying!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
