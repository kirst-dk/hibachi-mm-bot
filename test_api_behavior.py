#!/usr/bin/env python3
"""
Test script to verify API behavior and validate bot assumptions
Run this BEFORE starting the bot to ensure everything works correctly
"""

from hibachi_client import HibachiRest
from env_config import load_env_config
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test")


def test_contract_info(rest: HibachiRest, symbol: str):
    """Test 1: Verify contract specifications"""
    log.info("\n" + "=" * 70)
    log.info("TEST 1: Contract Information")
    log.info("=" * 70)

    contract = rest.get_contract_info(symbol)
    if not contract:
        log.error("❌ FAILED: Contract %s not found!", symbol)
        return False

    log.info("✅ Contract found: %s", symbol)
    log.info("  tick_size: %s", contract.get('tickSize') or contract.get('tick_size'))
    log.info("  step_size: %s", contract.get('stepSize') or contract.get('step_size'))
    log.info("  min_qty: %s", contract.get('minOrderSize') or contract.get('min_order_size'))
    log.info("  min_notional: %s", contract.get('minNotional') or contract.get('min_notional'))
    log.info("  contract_size: %s", contract.get('contractSize') or contract.get('contract_size'))
    return True


def test_price_data(rest: HibachiRest, symbol: str):
    """Test 2: Verify price data retrieval"""
    log.info("\n" + "=" * 70)
    log.info("TEST 2: Price Data")
    log.info("=" * 70)

    # Test get_prices
    prices = rest.get_prices(symbol)
    if prices:
        log.info("✅ get_prices() works")
        log.info("  markPrice: %s", prices.get('markPrice') or prices.get('mark_price'))
        log.info("  lastPrice: %s", prices.get('lastPrice') or prices.get('last_price'))
    else:
        log.warning("⚠️  get_prices() returned None")

    # Test get_mid_price
    mid = rest.get_mid_price(symbol)
    if mid:
        log.info("✅ get_mid_price() works: $%.2f", mid)
    else:
        log.error("❌ FAILED: get_mid_price() returned None")
        return False

    # Test orderbook
    orderbook = rest.get_orderbook(symbol, depth=1)
    if orderbook:
        bids = orderbook.get('bids', [])
        asks = orderbook.get('asks', [])
        if bids and asks:
            log.info("✅ Orderbook works")
            log.info("  Best bid: %s", bids[0])
            log.info("  Best ask: %s", asks[0])
        else:
            log.error("❌ FAILED: Empty orderbook")
            return False
    else:
        log.error("❌ FAILED: Could not fetch orderbook")
        return False

    return True


def test_account_data(rest: HibachiRest):
    """Test 3: Verify account data"""
    log.info("\n" + "=" * 70)
    log.info("TEST 3: Account Data")
    log.info("=" * 70)

    account = rest.get_account_info()
    if not account:
        log.error("❌ FAILED: Could not fetch account info")
        return False

    balance = float(account.get('balance', 0))
    log.info("✅ Account balance: $%.2f", balance)

    if balance < 10:
        log.warning("⚠️  WARNING: Low balance ($%.2f). Add funds before trading!", balance)

    positions = rest.get_positions()
    log.info("  Open positions: %d", len(positions))
    for pos in positions:
        log.info("    %s: size=%.6f", pos.get('symbol'), pos.get('size', 0))

    return True


def test_position_calculation(rest: HibachiRest, symbol: str):
    """Test 4: Verify position notional calculation"""
    log.info("\n" + "=" * 70)
    log.info("TEST 4: Position Notional Calculation")
    log.info("=" * 70)

    contract = rest.get_contract_info(symbol)
    position = rest.get_position(symbol)
    mid = rest.get_mid_price(symbol)

    if not contract or not mid:
        log.error("❌ FAILED: Missing data for calculation")
        return False

    contract_size = float(contract.get('contractSize') or contract.get('contract_size') or 1.0)

    if position:
        pos_qty = float(position.get('size', 0))

        # Test both formulas
        notional_with_cs = pos_qty * mid * contract_size
        notional_without_cs = pos_qty * mid

        log.info("  Position size: %.6f", pos_qty)
        log.info("  Mid price: $%.2f", mid)
        log.info("  Contract size: %.6f", contract_size)
        log.info("  Notional WITH contract_size: $%.2f", notional_with_cs)
        log.info("  Notional WITHOUT contract_size: $%.2f", notional_without_cs)

        # Check which makes sense
        account = rest.get_account_info()
        balance = float(account.get('balance', 0))

        if abs(notional_with_cs) > balance * 2:
            log.warning("⚠️  Notional WITH contract_size seems too large")
        if abs(notional_without_cs) > balance * 2:
            log.warning("⚠️  Notional WITHOUT contract_size seems too large")

        log.info("✅ Review the notional calculations above")
        log.info("   The correct formula should show reasonable notional relative to balance")
    else:
        log.info("✅ No position - skipping calculation")

    return True


def test_klines(rest: HibachiRest, symbol: str):
    """Test 5: Verify klines data"""
    log.info("\n" + "=" * 70)
    log.info("TEST 5: Historical Klines")
    log.info("=" * 70)

    klines = rest.get_klines(symbol, interval='5m', limit=5)
    if klines:
        log.info("✅ Klines available: %d candles", len(klines))
        if len(klines) > 0:
            last = klines[-1]
            log.info("  Last candle: O=%.2f H=%.2f L=%.2f C=%.2f",
                     float(last[1]), float(last[2]), float(last[3]), float(last[4]))
    else:
        log.warning("⚠️  Klines not available (ATR will initialize from live ticks)")

    return True


def main():
    log.info("=" * 70)
    log.info("HIBACHI BOT - API BEHAVIOR TEST")
    log.info("=" * 70)

    try:
        cfg = load_env_config()
    except Exception as e:
        log.error("❌ Failed to load config: %s", e)
        return 1

    symbol = cfg['bot']['symbol']
    log.info("Testing symbol: %s\n", symbol)

    rest = HibachiRest(
        api_url=cfg["api"]["apiUrl"],
        data_api_url=cfg["api"]["dataApiUrl"],
        api_key=cfg["api"]["apiKey"],
        account_id=cfg["api"]["accountId"],
        private_key=cfg["api"]["privateKey"]
    )

    # Run all tests
    results = []
    results.append(("Contract Info", test_contract_info(rest, symbol)))
    results.append(("Price Data", test_price_data(rest, symbol)))
    results.append(("Account Data", test_account_data(rest)))
    results.append(("Position Calculation", test_position_calculation(rest, symbol)))
    results.append(("Klines", test_klines(rest, symbol)))

    # Summary
    log.info("\n" + "=" * 70)
    log.info("TEST SUMMARY")
    log.info("=" * 70)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        log.info("%s: %s", status, name)

    log.info("\nResult: %d/%d tests passed", passed, total)

    if passed == total:
        log.info("\n🎉 All tests passed! Bot is ready to run.")
        log.info("Start with: python main.py")
        return 0
    else:
        log.error("\n❌ Some tests failed. Fix issues before running bot.")
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())