"""Async WebSocket client wrapper for Hibachi SDK 0.2.0"""
from __future__ import annotations
import asyncio
import logging
from typing import Optional, Callable, Dict, Any, List

log = logging.getLogger("hibachi.ws")

try:
    from hibachi_xyz import (
        HibachiWSMarketClient,
        HibachiWSTradeClient,
        HibachiWSAccountClient,
        WebSocketSubscription,
    )
    from hibachi_xyz.types import Side, WebSocketSubscriptionTopic, OrderFlags, OrderType

    _TOPIC_MAP = {t.value: t for t in WebSocketSubscriptionTopic}

    HAS_WS = True
except ImportError:
    HAS_WS = False
    log.warning("WebSocket classes not available. Install hibachi-xyz>=0.2.0")


class HibachiWS:
    """Unified async WebSocket client for market data, trading, and account updates."""

    def __init__(self, api_url: str, api_key: str, account_id: str,
                 private_key: str, public_key: str = "",
                 data_api_url: str = "https://data-api.hibachi.xyz"):
        if not HAS_WS:
            raise RuntimeError("WebSocket not available. Install hibachi-xyz>=0.2.0")

        self.api_url = api_url
        self.data_api_url = data_api_url
        self.api_key = api_key
        self.account_id = account_id
        self.private_key = private_key
        self.public_key = public_key

        self.market_ws: Optional[HibachiWSMarketClient] = None
        self.trade_ws: Optional[HibachiWSTradeClient] = None
        self.account_ws: Optional[HibachiWSAccountClient] = None

        # Serialize all Trade WS calls so multiple engines
        # sharing this client don't race each other.
        self._trade_lock: asyncio.Lock = asyncio.Lock()

        # Multiplexer: allow multiple handlers per market-data topic.
        # SDK only supports one handler per topic, so we fan-out manually.
        self._topic_handlers: Dict[str, List[Callable]] = {}

    async def connect_market(self):
        """Connect to the market data WebSocket."""
        self.market_ws = HibachiWSMarketClient(api_endpoint=self.data_api_url)
        await self.market_ws.connect()
        log.info("Market WebSocket connected")

    async def connect_trade(self):
        """Connect to the trade WebSocket for order management."""
        self.trade_ws = HibachiWSTradeClient(
            api_key=self.api_key,
            account_id=self.account_id,
            account_public_key=self.public_key,
            private_key=self.private_key,
            api_url=self.api_url,
            data_api_url=self.data_api_url
        )
        await self.trade_ws.connect()
        log.info("Trade WebSocket connected")

    async def reconnect_trade(self):
        """Reconnect Trade WebSocket and restore safety settings."""
        try:
            if self.trade_ws:
                await self.trade_ws.disconnect()
        except Exception:
            pass

        await self.connect_trade()
        try:
            await self.enable_cancel_on_disconnect()
        except Exception as e:
            log.warning("Failed to re-enable cancel-on-disconnect after reconnect: %s", e)

    async def connect_account(self) -> Any:
        """Connect to the account WebSocket for balance/position updates."""
        self.account_ws = HibachiWSAccountClient(
            api_endpoint=self.api_url,
            api_key=self.api_key,
            account_id=str(self.account_id)
        )
        await self.account_ws.connect()
        result = await self.account_ws.stream_start()
        log.info("Account WebSocket connected")
        return result

    async def enable_cancel_on_disconnect(self):
        """Enable automatic order cancellation when WS disconnects (safety feature)."""
        if not self.trade_ws:
            raise RuntimeError("Trade WS not connected")
        import time as _time
        try:
            from hibachi_xyz.types import EnableCancelOnDisconnectParams
            params = EnableCancelOnDisconnectParams(nonce=_time.time_ns() // 1_000)
        except ImportError:
            from hibachi_xyz import EnableCancelOnDisconnectParams
            params = EnableCancelOnDisconnectParams(nonce=_time.time_ns() // 1_000)

        result = await self.trade_ws.enable_cancel_on_disconnect(params)
        log.info("Cancel-on-disconnect enabled")
        return result

    async def subscribe(self, symbols: List[str], topics: List[str]):
        """Subscribe to market data topics for given symbols."""
        if not self.market_ws:
            raise RuntimeError("Market WS not connected")

        subs = []
        for symbol in symbols:
            for topic in topics:
                topic_enum = _TOPIC_MAP.get(topic)
                if topic_enum is None:
                    log.warning("Unknown WS topic: %s", topic)
                    continue
                subs.append(WebSocketSubscription(symbol=symbol, topic=topic_enum))

        if subs:
            await self.market_ws.subscribe(subs)
        log.info("Subscribed to %d topic(s) for %d symbol(s)",
                 len(topics), len(symbols))

    def on(self, topic: str, handler: Callable):
        """Register an async handler for a market data topic.
        Supports multiple handlers per topic via internal fan-out multiplexer.
        """
        if topic not in self._topic_handlers:
            self._topic_handlers[topic] = []
            # Register a single multiplexer with the SDK for this topic.
            # It will call ALL registered handlers when a message arrives.
            async def _mux(msg, _topic=topic):
                for h in self._topic_handlers.get(_topic, []):
                    try:
                        await h(msg)
                    except Exception as e:
                        log.error("Handler error for topic %s: %s", _topic, e)
            if self.market_ws:
                self.market_ws.on(topic, _mux)
        self._topic_handlers[topic].append(handler)

    async def listen_account(self) -> Optional[dict]:
        """Listen for account WebSocket messages (5s timeout with ping)."""
        if self.account_ws:
            return await self.account_ws.listen()
        return None

    async def place_limit_order(self, symbol: str, side_str: str,
                                quantity: float, price: float,
                                max_fees_percent: float = 0.001):
        """Place a limit order via Trade WebSocket. Returns (nonce, order_id)."""
        if not self.trade_ws:
            raise RuntimeError("Trade WS not connected")

        side_enum = Side.BUY if side_str.upper() == "BUY" else Side.SELL

        def _build_params():
            try:
                from hibachi_xyz import OrderPlaceParams
            except ImportError:
                from hibachi_xyz.types import OrderPlaceParams
            return OrderPlaceParams(
                symbol=symbol,
                side=side_enum,
                quantity=quantity,
                price=price,
                maxFeesPercent=max_fees_percent,
                orderType=OrderType.LIMIT,
                orderFlags=OrderFlags.PostOnly.value
            )

        last_err = None
        async with self._trade_lock:
            for attempt in range(2):
                try:
                    params = _build_params()
                    nonce, order_id = await self.trade_ws.place_order(params)
                    return nonce, order_id
                except Exception as e:
                    last_err = e
                    if attempt == 0:
                        log.warning("WS place failed, reconnecting Trade WS and retrying once: %s", e)
                        await self.reconnect_trade()
                        continue
                    break

        raise RuntimeError(f"WS place failed after reconnect: {last_err}")

    async def modify_order(self, order, quantity: float, price: float,
                           side_str: str, max_fees_percent: float = 0.001):
        """Modify an existing order via Trade WebSocket."""
        if not self.trade_ws:
            raise RuntimeError("Trade WS not connected")

        from hibachi_xyz import websockets as ws_types
        side_enum = ws_types.Side.BUY if side_str.upper() == "BUY" else ws_types.Side.SELL

        return await self.trade_ws.modify_order(
            order=order,
            quantity=quantity,
            price=str(price),
            side=side_enum,
            maxFeesPercent=max_fees_percent
        )

    async def cancel_order(self, order_id: int, nonce: int):
        """Cancel a specific order via Trade WebSocket."""
        if not self.trade_ws:
            raise RuntimeError("Trade WS not connected")
        async with self._trade_lock:
            return await self.trade_ws.cancel_order(orderId=order_id, nonce=nonce)

    async def cancel_all_orders(self):
        """Cancel all orders via Trade WebSocket."""
        if not self.trade_ws:
            raise RuntimeError("Trade WS not connected")
        async with self._trade_lock:
            return await self.trade_ws.cancel_all_orders()

    async def get_orders_status(self):
        """Get status of all orders via Trade WebSocket."""
        if not self.trade_ws:
            raise RuntimeError("Trade WS not connected")
        async with self._trade_lock:
            return await self.trade_ws.get_orders_status()

    async def disconnect(self):
        """Disconnect all WebSocket connections."""
        for name, ws in [("market", self.market_ws),
                         ("trade", self.trade_ws),
                         ("account", self.account_ws)]:
            if ws:
                try:
                    await ws.disconnect()
                    log.info("%s WS disconnected", name.capitalize())
                except Exception as e:
                    log.error("Error disconnecting %s WS: %s", name, e)
        self.market_ws = None
        self.trade_ws = None
        self.account_ws = None
