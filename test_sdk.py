#!/usr/bin/env python3
from hibachi_xyz import HibachiApiClient
from dotenv import load_dotenv
import os

load_dotenv()

client = HibachiApiClient(
    api_url=os.getenv("HIBACHI_API_ENDPOINT"),
    data_api_url=os.getenv("HIBACHI_DATA_API_ENDPOINT"),
    api_key=os.getenv("HIBACHI_API_KEY"),
    account_id=os.getenv("HIBACHI_ACCOUNT_ID"),
    private_key=os.getenv("HIBACHI_PRIVATE_KEY")
)

print("=" * 70)
print("AVAILABLE SDK METHODS:")
print("=" * 70)
methods = [m for m in dir(client) if not m.startswith('_') and callable(getattr(client, m))]
for method in sorted(methods):
    print(f"  - {method}")

print("\n" + "=" * 70)
print("Testing orderbook for BTC/USDT-P...")
print("=" * 70)
try:
    orderbook = client.get_orderbook(symbol="BTC/USDT-P")
    print(f"Type: {type(orderbook)}")
    if hasattr(orderbook, 'model_dump'):
        data = orderbook.model_dump()
    elif hasattr(orderbook, 'dict'):
        data = orderbook.dict()
    else:
        data = orderbook

    print(f"Bids (first 3): {data.get('bids', [])[:3]}")
    print(f"Asks (first 3): {data.get('asks', [])[:3]}")

    if data.get('bids') and data.get('asks'):
        best_bid = float(data['bids'][0][0]) if isinstance(data['bids'][0], list) else float(data['bids'][0])
        best_ask = float(data['asks'][0][0]) if isinstance(data['asks'][0], list) else float(data['asks'][0])
        mid = (best_bid + best_ask) / 2
        print(f"Mid price: ${mid:.2f}")
except Exception as e:
    print(f"Error: {e}")