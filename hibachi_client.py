from __future__ import annotations
import logging
from typing import Dict, Any, Optional, List

try:
    from hibachi_xyz import HibachiApiClient
    from hibachi_xyz.types import Side, Interval, OrderFlags
except ImportError:
    raise ImportError("Install: pip install hibachi-xyz")

_INTERVAL_MAP = {
    "1m": Interval.ONE_MINUTE, "1min": Interval.ONE_MINUTE,
    "5m": Interval.FIVE_MINUTES, "5min": Interval.FIVE_MINUTES,
    "15m": Interval.FIFTEEN_MINUTES, "15min": Interval.FIFTEEN_MINUTES,
    "1h": Interval.ONE_HOUR,
    "4h": Interval.FOUR_HOURS,
    "1d": Interval.ONE_DAY,
    "1w": Interval.ONE_WEEK,
}

try:
    from hibachi_xyz.types import CreateOrder, UpdateOrder, CancelOrder
    HAS_BATCH = True
except ImportError:
    HAS_BATCH = False

log = logging.getLogger("hibachi.client")


class HibachiRest:
    def __init__(self, api_url: str, data_api_url: str, api_key: str,
                 account_id: str, private_key: str):
        self.client = HibachiApiClient(
            api_url=api_url,
            data_api_url=data_api_url,
            api_key=api_key,
            account_id=account_id,
            private_key=private_key
        )
        log.info("Hibachi client initialized")

    def _convert_to_dict(self, obj: Any) -> Any:
        """Convert Pydantic model to dict recursively"""
        if hasattr(obj, 'model_dump'):
            return obj.model_dump()
        elif hasattr(obj, 'dict'):
            return obj.dict()
        elif hasattr(obj, '__dict__'):
            result = {}
            for key in dir(obj):
                if not key.startswith('_'):
                    val = getattr(obj, key, None)
                    if val is not None and not callable(val):
                        result[key] = val
            return result
        return obj

    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """Set leverage for symbol"""
        try:
            if hasattr(self.client, 'set_leverage'):
                result = self.client.set_leverage(
                    symbol=symbol,
                    leverage=leverage
                )
                log.info("✓ Leverage set to %dx for %s via SDK", leverage, symbol)
                return self._convert_to_dict(result)

            elif hasattr(self.client, 'update_leverage'):
                result = self.client.update_leverage(
                    symbol=symbol,
                    leverage=leverage
                )
                log.info("✓ Leverage set to %dx for %s via update_leverage", leverage, symbol)
                return self._convert_to_dict(result)

            elif hasattr(self.client, 'change_leverage'):
                result = self.client.change_leverage(
                    symbol=symbol,
                    leverage=leverage
                )
                log.info("✓ Leverage set to %dx for %s via change_leverage", leverage, symbol)
                return self._convert_to_dict(result)

            else:
                log.warning("⚠ SDK does not support leverage methods")
                log.warning("⚠ Available methods: %s",
                            [m for m in dir(self.client) if not m.startswith('_')])
                return {"status": "not_supported", "message": "Manual setup required"}

        except Exception as e:
            log.error("Failed to set leverage: %s", e)
            log.error("You MUST manually set leverage to %dx in Hibachi web interface!", leverage)
            raise

    def get_account_info(self) -> Dict[str, Any]:
        """Get account information including balance"""
        result = self.client.get_account_info()
        return self._convert_to_dict(result)

    def get_balance(self) -> float:
        info = self.get_account_info()
        return float(info.get('balance', 0))

    def get_exchange_info(self) -> Dict[str, Any]:
        """Get exchange information including all trading pairs"""
        result = self.client.get_exchange_info()
        return self._convert_to_dict(result)

    def get_contract_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get specific contract information"""
        info = self.get_exchange_info()

        contracts = (info.get('futureContracts') or
                     info.get('future_contracts') or
                     info.get('contracts') or [])

        for contract in contracts:
            contract_dict = self._convert_to_dict(contract)
            if contract_dict.get('symbol') == symbol:
                return contract_dict

        available = [self._convert_to_dict(c).get('symbol') for c in contracts]
        log.error(f"Contract {symbol} not found. Available: {available}")
        return None

    def get_symbols(self) -> List[str]:
        """Get all tradeable symbol strings from inventory"""
        try:
            result = self.client.get_inventory()
            symbols = []
            for market in (getattr(result, 'markets', None) or []):
                contract = getattr(market, 'contract', None)
                if contract:
                    sym = str(getattr(contract, 'symbol', ''))
                    if sym and '/' in sym:
                        symbols.append(sym)
            return symbols
        except Exception as e:
            log.debug("get_symbols failed: %s", e)
            return []

    def get_symbols_with_prices(self) -> Dict[str, Optional[float]]:
        """Get all symbols with their current mark prices in one inventory call."""
        try:
            result = self.client.get_inventory()
            out: Dict[str, Optional[float]] = {}
            for market in (getattr(result, 'markets', None) or []):
                contract = getattr(market, 'contract', None)
                if not contract:
                    continue
                sym = str(getattr(contract, 'symbol', ''))
                if not sym or '/' not in sym:
                    continue
                info = getattr(market, 'info', None)
                price = None
                if info:
                    for field in ('markPrice', 'priceLatest', 'price24hAgo'):
                        raw = getattr(info, field, None)
                        if raw:
                            try:
                                price = float(raw)
                                break
                            except (TypeError, ValueError):
                                pass
                out[sym] = price
            return out
        except Exception as e:
            log.debug("get_symbols_with_prices failed: %s", e)
            return {}

    def get_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions from account info"""
        try:
            result = self.client.get_account_info()
            raw_positions = getattr(result, 'positions', []) or []
            out = []
            for p in raw_positions:
                qty = float(getattr(p, 'quantity', 0) or 0)
                direction = str(getattr(p, 'direction', 'Long'))
                # Signed size: positive = Long, negative = Short
                size = qty if direction.lower() == 'long' else -qty
                upnl = (float(getattr(p, 'unrealizedTradingPnl', 0) or 0) +
                        float(getattr(p, 'unrealizedFundingPnl', 0) or 0))
                out.append({
                    'symbol':               str(getattr(p, 'symbol', '')),
                    'size':                 size,
                    'quantity':             size,
                    'direction':            direction,
                    'entryPrice':           float(getattr(p, 'openPrice', 0) or 0),
                    'markPrice':            float(getattr(p, 'markPrice', 0) or 0),
                    'notionalValue':        float(getattr(p, 'notionalValue', 0) or 0),
                    'unrealizedPnl':        upnl,
                    'unrealizedTradingPnl': float(getattr(p, 'unrealizedTradingPnl', 0) or 0),
                    'unrealizedFundingPnl': float(getattr(p, 'unrealizedFundingPnl', 0) or 0),
                })
            return [pos for pos in out if pos['size'] != 0]
        except Exception as e:
            log.debug("No positions found: %s", e)
            return []

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get position for specific symbol"""
        for pos in self.get_positions():
            if pos.get('symbol') == symbol:
                return pos
        return None

    def get_orderbook(self, symbol: str, depth: int = 5,
                      granularity: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Get orderbook with required parameters"""
        try:
            if granularity is None:
                contract = self.get_contract_info(symbol)
                if contract:
                    granularity = float(contract.get('tickSize') or
                                        contract.get('tick_size') or 0.01)
                else:
                    granularity = 0.01

            result = self.client.get_orderbook(
                symbol=symbol,
                depth=depth,
                granularity=granularity
            )
            return self._convert_to_dict(result)
        except Exception as e:
            log.error("Failed to get orderbook for %s: %s", symbol, e)
            return None

    def get_prices(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current prices for symbol"""
        try:
            if hasattr(self.client, 'get_prices'):
                result = self.client.get_prices(symbol=symbol)
                return self._convert_to_dict(result)
            return None
        except Exception as e:
            log.debug("Failed to get prices: %s", e)
            return None

    def get_mid_price(self, symbol: str) -> Optional[float]:
        """Get mid price - tries multiple methods"""
        mid = self._get_mid_from_prices(symbol)
        if mid:
            return mid

        mid = self._get_mid_from_orderbook(symbol)
        if mid:
            return mid

        log.warning("Unable to get mid price for %s", symbol)
        return None

    def _get_mid_from_prices(self, symbol: str) -> Optional[float]:
        """Get mid price from prices API"""
        try:
            prices = self.get_prices(symbol)
            if not prices:
                return None

            mark = prices.get('markPrice') or prices.get('mark_price')
            if mark:
                return float(mark)

            last = prices.get('lastPrice') or prices.get('last_price')
            if last:
                return float(last)

            return None
        except Exception as e:
            log.debug("Failed to get mid from prices: %s", e)
            return None

    def _get_mid_from_orderbook(self, symbol: str) -> Optional[float]:
        """Get mid price from orderbook"""
        try:
            orderbook = self.get_orderbook(symbol, depth=1)
            if not orderbook:
                return None

            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])

            if not bids or not asks:
                return None

            best_bid = self._parse_orderbook_level(bids[0])
            best_ask = self._parse_orderbook_level(asks[0])

            if best_bid and best_ask and best_bid > 0 and best_ask > 0:
                return (best_bid + best_ask) / 2

            return None
        except Exception as e:
            log.debug("Failed to get mid from orderbook: %s", e)
            return None

    def _parse_orderbook_level(self, level) -> Optional[float]:
        """Parse orderbook level (supports list, dict, or scalar)"""
        try:
            if isinstance(level, list):
                return float(level[0])
            elif isinstance(level, dict):
                return float(level.get('price', 0))
            else:
                return float(level)
        except (ValueError, TypeError):
            return None

    def get_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get ticker - uses get_prices"""
        prices = self.get_prices(symbol)
        if prices:
            return prices

        mid = self.get_mid_price(symbol)
        if mid:
            return {
                'symbol': symbol,
                'lastPrice': mid,
                'markPrice': mid
            }
        return None

    def place_order(self, symbol: str, side: str, order_type: str,
                    quantity: str, price: Optional[str] = None,
                    time_in_force: str = 'GTC', reduce_only: bool = False,
                    post_only: bool = False,
                    client_order_id: Optional[str] = None) -> Dict[str, Any]:

        side_upper = side.upper()
        if side_upper == "BUY":
            side_enum = Side.BUY
        elif side_upper == "SELL":
            side_enum = Side.SELL
        else:
            raise ValueError(f"Invalid side: {side}. Must be 'BUY' or 'SELL'")

        max_fees_percent = 0.01

        if order_type == "LIMIT" and price:
            try:
                kwargs_limit = dict(
                    symbol=symbol,
                    side=side_enum,
                    quantity=float(quantity),
                    price=float(price),
                    max_fees_percent=max_fees_percent
                )
                if post_only:
                    kwargs_limit["order_flags"] = OrderFlags.PostOnly
                result = self.client.place_limit_order(**kwargs_limit)

                # API возвращает tuple (timestamp, order_id)
                if isinstance(result, tuple):
                    log.debug("API returned tuple, length: %d", len(result))
                    if len(result) >= 2:
                        # Создаем dict с order_id из tuple
                        result = {"orderId": str(result[-1])}
                        log.debug("Extracted order_id from tuple: %s", result["orderId"])
                    else:
                        result = {"orderId": str(result[0])} if result else {}

                return self._convert_to_dict(result)
            except Exception as e:
                log.error(f"place_limit_order failed: {e}")
                log.error(f"  Symbol: {symbol}, Side: {side}")
                log.error(f"  Price: {price}, Quantity: {quantity}")
                raise
        elif order_type == "MARKET":
            result = self.client.place_market_order(
                symbol=symbol,
                side=side_enum,
                quantity=float(quantity),
                max_fees_percent=max_fees_percent
            )

            if isinstance(result, tuple):
                log.debug("API returned tuple, length: %d", len(result))
                if len(result) >= 2:
                    result = {"orderId": str(result[-1])}
                    log.debug("Extracted order_id from tuple: %s", result["orderId"])
                else:
                    result = {"orderId": str(result[0])} if result else {}

            return self._convert_to_dict(result)
        else:
            raise ValueError(f"Unsupported order type: {order_type}")

    def cancel_order(self, symbol: str, order_id: Optional[str] = None,
                     client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Cancel order - SDK requires INTEGER order_id"""
        try:
            if order_id:
                # КРИТИЧНО: SDK принимает только INTEGER, конвертируем из STRING
                oid_int = int(order_id)
                result = self.client.cancel_order(order_id=oid_int)
                log.debug("Canceled order_id %d", oid_int)
            elif client_order_id:
                result = self.client.cancel_order(client_order_id=client_order_id)
            else:
                raise ValueError("Need order_id or client_order_id")

            return self._convert_to_dict(result)
        except ValueError as e:
            log.error("Invalid order_id format '%s': %s", order_id, e)
            return {"status": "error", "message": f"Invalid order_id: {order_id}"}
        except Exception as e:
            log.error("Cancel order failed for order_id %s: %s", order_id, e)
            return {"status": "error", "message": str(e)}

    def cancel_all_orders(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Cancel all orders for symbol"""
        try:
            if symbol:
                try:
                    result = self.client.cancel_all_orders(symbol=symbol)
                    return self._convert_to_dict(result)
                except TypeError:
                    pass

            result = self.client.cancel_all_orders()
            return self._convert_to_dict(result)
        except Exception as e:
            log.error("Failed to cancel all orders: %s", e)
            return {"status": "error", "message": str(e)}

    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        try:
            if hasattr(self.client, 'get_pending_orders'):
                result = self.client.get_pending_orders(symbol=symbol) if symbol else self.client.get_pending_orders()
            else:
                result = self.client.get_open_orders(symbol=symbol) if symbol else self.client.get_open_orders()

            if not result:
                return []

            data = self._convert_to_dict(result)

            # SDK may return:
            # 1) list[Order]
            # 2) PendingOrdersResponse {"orders": [...]}
            # 3) single order object
            if isinstance(data, list):
                return [self._convert_to_dict(order) for order in data]

            if isinstance(data, dict):
                orders = data.get("orders")
                if isinstance(orders, list):
                    return [self._convert_to_dict(order) for order in orders]
                return [data]

            return [self._convert_to_dict(data)]
        except Exception as e:
            log.error("Failed to get open orders: %s", e)
            return []

    def get_klines(self, symbol: str, interval: str = '5m') -> Optional[List]:
        """Get historical klines/candles"""
        try:
            if not hasattr(self.client, 'get_klines'):
                log.warning("SDK does not support get_klines")
                return None
            iv = _INTERVAL_MAP.get(interval)
            if iv is None:
                log.warning("Unknown klines interval: %s", interval)
                return None
            result = self.client.get_klines(symbol=symbol, interval=iv)
            if hasattr(result, 'klines'):
                return [self._convert_to_dict(k) for k in result.klines]
            return self._convert_to_dict(result)
        except Exception as e:
            log.debug("Failed to get klines: %s", e)
            return None

    def update_order(self, order_id: int, price: Optional[float] = None,
                     quantity: Optional[float] = None,
                     max_fees_percent: float = 0.01) -> Dict[str, Any]:
        """Update an existing order's price/quantity in-place"""
        try:
            kwargs = {"order_id": int(order_id), "max_fees_percent": max_fees_percent}
            if price is not None:
                kwargs["price"] = price
            if quantity is not None:
                kwargs["quantity"] = quantity
            result = self.client.update_order(**kwargs)
            return self._convert_to_dict(result)
        except Exception as e:
            log.error("Update order %s failed: %s", order_id, e)
            raise

    def batch_orders(self, orders: list) -> Dict[str, Any]:
        """Execute batch order operations (create/update/cancel) atomically"""
        if not HAS_BATCH:
            raise RuntimeError("Batch orders not available. Update SDK: pip install hibachi-xyz>=0.2.0")
        try:
            result = self.client.batch_orders(orders)
            return self._convert_to_dict(result)
        except Exception as e:
            log.error("Batch orders failed: %s", e)
            raise

    def get_account_trades(self) -> List[Dict[str, Any]]:
        """Get recent trade history for the account"""
        try:
            result = self.client.get_account_trades()
            data = self._convert_to_dict(result)
            trades = data.get('trades', [])
            if isinstance(trades, list):
                return [self._convert_to_dict(t) for t in trades]
            return []
        except Exception as e:
            log.debug("Failed to get account trades: %s", e)
            return []

    def get_capital_balance(self) -> float:
        """Get net equity including unrealized PnL"""
        try:
            if hasattr(self.client, 'get_capital_balance'):
                result = self.client.get_capital_balance()
                data = self._convert_to_dict(result)
                return float(data.get('balance', 0))
            return self.get_balance()
        except Exception as e:
            log.debug("Failed to get capital balance: %s", e)
            return self.get_balance()

    def get_order_details(self, order_id: int) -> Optional[Dict[str, Any]]:
        """Get details of a specific order"""
        try:
            result = self.client.get_order_details(order_id=int(order_id))
            return self._convert_to_dict(result)
        except Exception as e:
            log.debug("Failed to get order details for %s: %s", order_id, e)
            return None

    def close_position(self, symbol: str, size: float) -> Dict[str, Any]:
        """Close an open position with a market order in the opposite direction.

        Args:
            symbol: Trading pair, e.g. 'BTC/USDT-P'
            size:   Current position size (positive = long, negative = short)

        Returns:
            Order result dict with 'orderId' key.
        """
        if size == 0:
            return {"status": "noop", "message": "Position size is zero"}

        close_side = Side.SELL if size > 0 else Side.BUY
        qty = abs(size)

        # Attempt with ReduceOnly flag first; fall back without it if SDK
        # does not support order_flags for market orders.
        kwargs: Dict[str, Any] = dict(
            symbol=symbol,
            side=close_side,
            quantity=qty,
            max_fees_percent=0.01,
        )
        try:
            from hibachi_xyz.types import OrderFlags as _OF
            kwargs["order_flags"] = _OF.ReduceOnly
            result = self.client.place_market_order(**kwargs)
        except TypeError:
            log.debug("place_market_order does not accept order_flags; retrying without")
            kwargs.pop("order_flags", None)
            result = self.client.place_market_order(**kwargs)

        if isinstance(result, tuple):
            result = {"orderId": str(result[-1])} if len(result) >= 2 else {}

        log.info("close_position %s: %s %.4f → %s",
                 symbol, "SELL" if size > 0 else "BUY", qty, result)
        return self._convert_to_dict(result)