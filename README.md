# Hibachi Market Maker Bot

Futures market making bot for Hibachi Exchange.

## Features

- ATR-based dynamic spread
- Inventory skew management
- Position-aware sizing
- Futures contract support
- Rate limiting & safety checks

## Quick Start

### 1. Install
```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python check_dependencies.py