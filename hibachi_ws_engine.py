"""WebSocket-based Market Maker Engine for Hibachi.

Uses WS for real-time market data and order management.
REST client is used only for bootstrap (contract info, ATR, initial equity).
"""
from __future__ import annotations
import asyncio
import csv
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

from utils import ATR, bps_to_price, pct_of, clamp, ContractSpec, get_precision
from hibachi_client import HibachiRest
from hibachi_ws_client import HibachiWS

log = logging.getLogger("hibachi.ws_mm")


@dataclass
class WSSideState:
    order_id: Optional[int] = None
    nonce: Optional[int] = None
    price: Optional[float] = None
    qty: Optional[float] = None
    side: str = ""
    placed_at: float = 0.0  # time.time() when order was sent


@dataclass
class WSMMState:
    bid: WSSideState = field(default_factory=WSSideState)
    ask: WSSideState = field(default_factory=WSSideState)
    prev_mid: Optional[float] = None
    equity_usd: float = 0.0
    pos_qty: float = 0.0
    mark_price: float = 0.0
    atr_val: float = 0.0
    last_bar_high: float = 0.0
    last_bar_low: float = 0.0
    last_bar_close: float = 0.0
    quote_count: int = 0
    total_volume: float = 0.0
    realized_pnl: float = 0.0
    fills_count: int = 0
    price_changed: bool = False
    last_requote: float = 0.0


class HibachiWSMarketMakerEngine:
    """Async WebSocket-based market maker engine."""

    def __init__(self, ws: HibachiWS, rest: HibachiRest,
                 cfg: dict, logs_dir: str):
        self.ws = ws
        self.rest = rest
        self.cfg = cfg
        self.symbol = cfg["symbol"]
        self.params = cfg
        self.state = WSMMState()
        self.contract: Optional[ContractSpec] = None
        self.atr = ATR(int(cfg["atrLen"]))
        self.shutdown = False
        self.trades_log_path = os.path.join(logs_dir, f"trades_{self.symbol.replace('/', '_')}.csv")
        self._ensure_trade_log()

        self.target_leverage = 1
        self.min_requote_interval = 0.5  # Don't requote faster than 0.5s

    def _ensure_trade_log(self):
        if not os.path.exists(self.trades_log_path):
            os.makedirs(os.path.dirname(self.trades_log_path), exist_ok=True)
            with open(self.trades_log_path, "w", newline="") as f:
                csv.writer(f).writerow([
                    "ts", "symbol", "side", "price", "qty", "fee",
                    "orderId", "realizedPnl"
                ])

    def bootstrap(self):
        """Bootstrap using REST API (contract info, ATR, equity)."""
        log.info("[%s] Loading contract info...", self.symbol)
        contract_info = self.rest.get_contract_info(self.symbol)
        if not contract_info:
            raise RuntimeError(f"Contract {self.symbol} not found")

        tick_size = contract_info.get('tickSize') or contract_info.get('tick_size') or 0.01
        step_size = contract_info.get('stepSize') or contract_info.get('step_size') or 0.001
        min_qty = contract_info.get('minOrderSize') or contract_info.get('min_order_size') or 0.001
        min_notional = contract_info.get('minNotional') or contract_info.get('min_notional') or 10.0
        contract_size = contract_info.get('contractSize') or contract_info.get('contract_size') or 1.0

        self.contract = ContractSpec(
            symbol=self.symbol,
            tick_size=float(tick_size),
            step_size=float(step_size),
            min_qty=float(min_qty),
            min_notional=float(min_notional),
            contract_size=float(contract_size)
        )
        log.info("[%s] Contract: tick=%.6f step=%.6f min_notional=%.2f",
                 self.symbol, self.contract.tick_size,
                 self.contract.step_size, self.contract.min_notional)

        # Set leverage
        try:
            self.rest.set_leverage(self.symbol, self.target_leverage)
        except Exception as e:
            log.warning("[%s] Set leverage failed: %s", self.symbol, e)

        # Cancel existing orders and verify they are gone
        for attempt in range(3):
            try:
                self.rest.cancel_all_orders()
            except Exception as e:
                log.warning("[%s] cancel_all_orders attempt %d failed: %s", self.symbol, attempt + 1, e)
            time.sleep(1.0)
            open_orders = self.rest.get_open_orders()
            sym_orders = [
                o for o in open_orders
                if self._normalize_symbol(str(o.get("symbol") or o.get("market", ""))) == self._normalize_symbol(self.symbol)
            ]
            if not sym_orders:
                log.info("[%s] All existing orders cancelled", self.symbol)
                break
            log.warning("[%s] %d order(s) still open after cancel attempt %d, retrying...",
                        self.symbol, len(sym_orders), attempt + 1)
        else:
            log.warning("[%s] Could not cancel all orders after 3 attempts, proceeding anyway", self.symbol)

        # Bootstrap ATR
        try:
            klines = self.rest.get_klines(
                self.symbol,
                interval=self.params.get("atrTimeframe", "5m")
            )
            if klines:
                atr_len = int(self.params["atrLen"])
                klines = klines[-(atr_len + 10):]
                for candle in klines:
                    if isinstance(candle, dict):
                        o = float(candle.get('open', 0))
                        h = float(candle.get('high', 0))
                        l = float(candle.get('low', 0))
                        c = float(candle.get('close', 0))
                    elif isinstance(candle, (list, tuple)) and len(candle) >= 5:
                        o, h, l, c = (float(candle[1]), float(candle[2]),
                                      float(candle[3]), float(candle[4]))
                    else:
                        continue
                    if c > 0:
                        self.atr.update_bar(o, h, l, c, closed=True)
                if self.atr.rma:
                    log.info("[%s] ATR initialized: %.2f", self.symbol, self.atr.rma)
        except Exception as e:
            log.warning("[%s] ATR bootstrap failed: %s", self.symbol, e)

        # Bootstrap equity
        self.state.equity_usd = self.rest.get_capital_balance()
        position = self.rest.get_position(self.symbol)
        if position:
            self.state.pos_qty = float(position.get('size', 0))
        mid = self.rest.get_mid_price(self.symbol)
        if mid:
            self.state.mark_price = mid
        log.info("[%s] Balance: $%.2f | Position: %.4f",
                 self.symbol, self.state.equity_usd, self.state.pos_qty)

    async def run(self):
        """Main async loop: subscribe to data, react to price changes."""
        self.bootstrap()

        # Register price handler BEFORE subscribing
        _first_msg_logged = False

        _my_symbol_norm = self._normalize_symbol(self.symbol)

        async def on_mark_price(msg):
            nonlocal _first_msg_logged
            if isinstance(msg, dict):
                # Filter: only handle messages for THIS symbol.
                # Multiple engines share one WS client; each gets all messages.
                msg_sym = msg.get('symbol') or msg.get('market') or ''
                if msg_sym and self._normalize_symbol(str(msg_sym)) != _my_symbol_norm:
                    return

                if not _first_msg_logged:
                    log.info("[%s] First WS mark_price msg: %s", self.symbol, msg)
                    _first_msg_logged = True

                price = msg.get('markPrice') or msg.get('mark_price')
                if not price and 'data' in msg:
                    d = msg['data']
                    if isinstance(d, dict):
                        price = d.get('markPrice') or d.get('mark_price')

                if price:
                    price = float(price)
                    if price != self.state.mark_price:
                        self.state.mark_price = price
                        self.state.price_changed = True

        self.ws.on("mark_price", on_mark_price)

        # Subscribe to market data for this symbol
        await self.ws.subscribe([self.symbol], ["mark_price"])

        log.info("[%s] WS engine running", self.symbol)

        # Initialize equity update timer
        last_equity_update = time.time()
        last_orders_sync = time.time()
        force_requote_sec = float(self.params.get("forceRequoteSec", 5.0))

        # Ensure first quote cycle runs even before the first WS tick.
        self.state.price_changed = True

        # Main loop: check for price changes and requote
        while not self.shutdown:
            try:
                now = time.time()

                # Keep quoting in flat market too.
                if now - self.state.last_requote >= force_requote_sec:
                    self.state.price_changed = True

                # Reconcile local side-state with exchange open orders.
                if now - last_orders_sync >= 1.0:
                    await self._sync_open_orders_state()
                    last_orders_sync = now

                if self.state.price_changed:
                    self.state.price_changed = False
                    if now - self.state.last_requote >= self.min_requote_interval:
                        await self._maybe_requote()
                        self.state.last_requote = now

                # Periodic equity update (every 60s)
                if now - last_equity_update > 60:
                    self.state.equity_usd = await asyncio.to_thread(self.rest.get_capital_balance)
                    position = await asyncio.to_thread(self.rest.get_position, self.symbol)
                    if position:
                        self.state.pos_qty = float(position.get('size', 0))
                    last_equity_update = now

            except Exception as e:
                log.error("[%s] WS engine error: %s", self.symbol, e)

            await asyncio.sleep(0.1)

        # Cleanup
        try:
            await self.ws.cancel_all_orders()
        except Exception:
            pass

    async def _sync_open_orders_state(self):
        """Reconcile local bid/ask state with exchange open orders."""
        orders = await self._get_symbol_open_orders()
        now = time.time()
        # Grace period: a freshly placed order may not yet appear in the
        # REST endpoint. Don't report it as "missing" for 4 seconds.
        _grace = 4.0

        # Hard safety: keep at most one BUY and one SELL order for this symbol.
        if len(orders) > 2:
            buy_orders = [o for o in orders if o["side"] == "BUY"]
            sell_orders = [o for o in orders if o["side"] == "SELL"]

            keep_ids = set()
            if buy_orders:
                keep_ids.add(max(buy_orders, key=lambda x: x["id"])["id"])
            if sell_orders:
                keep_ids.add(max(sell_orders, key=lambda x: x["id"])["id"])

            extras = [o for o in orders if o["id"] not in keep_ids]
            for extra in extras:
                try:
                    await asyncio.to_thread(self.rest.cancel_order, self.symbol, str(extra["id"]), None)
                    log.warning("[%s] Cancel duplicate %s order id=%s",
                                self.symbol, extra["side"], extra["id"])
                except Exception as e:
                    log.warning("[%s] Failed to cancel duplicate order id=%s: %s",
                                self.symbol, extra["id"], e)

            orders = await self._get_symbol_open_orders()

        active_ids = {o["id"] for o in orders}

        if (self.state.bid.order_id
                and now - self.state.bid.placed_at >= _grace
                and int(self.state.bid.order_id) not in active_ids):
            log.info("[%s] BID order %s filled/gone, replenishing immediately", self.symbol, self.state.bid.order_id)
            self.state.bid = WSSideState()
            self.state.prev_mid = None  # bypass price threshold, place at current market
            self.state.price_changed = True

        if (self.state.ask.order_id
                and now - self.state.ask.placed_at >= _grace
                and int(self.state.ask.order_id) not in active_ids):
            log.info("[%s] ASK order %s filled/gone, replenishing immediately", self.symbol, self.state.ask.order_id)
            self.state.ask = WSSideState()
            self.state.prev_mid = None  # bypass price threshold, place at current market
            self.state.price_changed = True

    async def _get_symbol_open_orders(self):
        """Return normalized open orders for current symbol: [{id, side}, ...]."""
        raw_orders = await asyncio.to_thread(self.rest.get_open_orders)
        if not isinstance(raw_orders, list):
            return []

        target_symbol = self._normalize_symbol(self.symbol)
        out = []
        for order in raw_orders:
            if not isinstance(order, dict):
                continue
            symbol = order.get("symbol") or order.get("market")
            if self._normalize_symbol(str(symbol)) != target_symbol:
                continue

            oid = (order.get("orderId") or order.get("order_id") or
                   order.get("id") or order.get("order"))
            raw_side = order.get("side")
            # SDK may return side as enum object (Side.BUY/Side.SELL), not string.
            side = self._normalize_side(raw_side)
            if side in ("BID",):
                side = "BUY"
            elif side in ("ASK",):
                side = "SELL"

            try:
                oid_int = int(oid)
            except (TypeError, ValueError):
                continue

            if side not in ("BUY", "SELL"):
                continue

            out.append({"id": oid_int, "side": side})

        return out

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        """Normalize symbol formats, e.g. HYPE/USDT-P -> HYPEUSDT."""
        if not symbol:
            return ""
        s = symbol.upper().strip()
        s = s.replace("-P", "")
        s = s.replace("/", "")
        s = s.replace("-", "")
        s = s.replace("_", "")
        return s

    @staticmethod
    def _normalize_side(raw_side) -> str:
        """Normalize side from SDK enum/string to canonical uppercase string."""
        if raw_side is None:
            return ""
        if isinstance(raw_side, str):
            return raw_side.upper().strip()

        # Enum-like objects from SDK, e.g. Side.BUY / Side.BID
        val = getattr(raw_side, "value", None)
        if isinstance(val, str):
            return val.upper().strip()

        name = getattr(raw_side, "name", None)
        if isinstance(name, str):
            return name.upper().strip()

        return str(raw_side).upper().strip()

    async def _maybe_requote(self):
        """Recalculate and update quotes based on new price data."""
        m = self.state.mark_price
        if not m or m <= 0 or not self.contract:
            return

        # Check if mid changed enough — but ONLY when both orders are already
        # active. If any side is missing we must place a replacement immediately
        # regardless of how much the price has moved.
        both_active = bool(self.state.bid.order_id) and bool(self.state.ask.order_id)
        if both_active and self.state.prev_mid is not None:
            threshold = bps_to_price(m, self.params.get("requoteBps", 10.0))
            if abs(m - self.state.prev_mid) <= threshold:
                return

        atr_val = self.atr.rma if self.atr.rma else m * 0.001
        atr_val = max(atr_val, m * 0.0005)

        equity = self.state.equity_usd
        if equity <= 0:
            return

        position_notional = self.state.pos_qty * m
        max_notional = equity * (self.params["invBudgetPct"] / 100)
        if max_notional <= 0:
            return

        skew = clamp(position_notional / max_notional, -1.0, 1.0)

        half_floor = bps_to_price(m, self.params["minFullBps"] * 0.5)
        half_ceil = bps_to_price(m, self.params["maxFullBps"] * 0.5)
        half_w = clamp(self.params["kATR"] * atr_val, half_floor, half_ceil)

        bull_shift = (bps_to_price(m, self.params.get("bullBiasBps", 0))
                      if self.params.get("useBullBias", False) else 0.0)

        sgn = 1.0 if skew > 0 else (-1.0 if skew < 0 else 0.0)
        inv_offset = self.params.get("skewDamp", 0.3) * abs(skew) * half_w * sgn

        ask_px = m + half_w + bull_shift - inv_offset
        bid_px = m - half_w + bull_shift - inv_offset

        # Minimum spread safety
        min_offset = m * 0.0015
        if bid_px >= m - min_offset:
            bid_px = m - min_offset
        if ask_px <= m + min_offset:
            ask_px = m + min_offset

        base_usd = pct_of(equity, self.params["baseOrderPct"])
        size_amp = self.params.get("sizeAmp", 1.5)
        bid_mult = 1.0 + size_amp * max(0.0, -skew)
        ask_mult = 1.0 + size_amp * max(0.0, skew)

        bid_notional = base_usd * bid_mult
        ask_notional = base_usd * ask_mult

        if bid_px <= 0 or ask_px <= 0:
            return

        bid_qty = self.contract.q_qty(bid_notional / bid_px)
        ask_qty = self.contract.q_qty(ask_notional / ask_px)
        bid_px_q = self.contract.q_price_floor(bid_px)
        ask_px_q = self.contract.q_price_ceil(ask_px)

        if bid_px_q >= m:
            bid_px_q = self.contract.q_price_floor(m - min_offset)
        if ask_px_q <= m:
            ask_px_q = self.contract.q_price_ceil(m + min_offset)

        can_buy = position_notional < max_notional
        can_sell = (self.state.pos_qty > 0 if self.params.get("longBiasOnly", False)
                    else position_notional > -max_notional)

        min_not = self.contract.min_notional
        bid_ok = (bid_qty >= self.contract.min_qty and bid_px_q * bid_qty >= min_not)
        ask_ok = (ask_qty >= self.contract.min_qty and ask_px_q * ask_qty >= min_not)

        log.info("[%s] mid=%.2f bid=%.2f ask=%.2f skew=%.3f pos=%.4f eq=$%.0f",
                 self.symbol, m, bid_px_q, ask_px_q, skew,
                 self.state.pos_qty, equity)

        max_fees = 0.001

        # Update or place BID
        if can_buy and bid_ok:
            await self._ws_place_or_cancel("BUY", bid_px_q, bid_qty, max_fees, "bid")
        elif self.state.bid.order_id:
            await self._ws_cancel_side("bid")

        # Update or place ASK
        if can_sell and ask_ok:
            await self._ws_place_or_cancel("SELL", ask_px_q, ask_qty, max_fees, "ask")
        elif self.state.ask.order_id:
            await self._ws_cancel_side("ask")

        self.state.prev_mid = m
        self.state.quote_count += 1

    async def _ws_place_or_cancel(self, side: str, price: float, qty: float,
                                   max_fees: float, state_key: str):
        """Cancel existing order and place new one via WS."""
        st = self.state.bid if state_key == "bid" else self.state.ask

        # Cancel old order first
        if st.order_id:
            cancelled = await self._ws_cancel_side(state_key)
            if not cancelled:
                # Do not place a new order if old one could not be cancelled,
                # otherwise duplicates accumulate on the exchange.
                log.warning("[%s] Skip placing %s: old %s order %s not cancelled",
                            self.symbol, side, state_key.upper(), st.order_id)
                return

        # Place new order
        try:
            nonce, order_id = await self.ws.place_limit_order(
                symbol=self.symbol,
                side_str=side,
                quantity=qty,
                price=price,
                max_fees_percent=max_fees
            )

            new_state = WSSideState(
                order_id=int(order_id),
                nonce=int(nonce),
                price=price,
                qty=qty,
                side=side,
                placed_at=time.time()
            )

            if state_key == "bid":
                self.state.bid = new_state
            else:
                self.state.ask = new_state

            log.info("[%s] WS PLACE %s @ %.2f x %.6f -> oid=%s",
                     self.symbol, side, price, qty, order_id)

        except Exception as e:
            log.error("[%s] WS place %s failed: %s", self.symbol, side, e)
            # Fallback: place the order via REST so quoting keeps running.
            try:
                res = await asyncio.to_thread(
                    self.rest.place_order,
                    self.symbol,
                    side,
                    "LIMIT",
                    f"{qty:.8f}",
                    f"{price:.8f}",
                    self.params.get("timeInForce", "GTC"),
                    False,
                    bool(self.params.get("postOnly", True)),
                    None,
                )
                oid = None
                if isinstance(res, dict):
                    oid = (res.get("orderId") or res.get("order_id") or
                           res.get("id") or res.get("orderID") or res.get("order"))
                if oid is not None:
                    new_state = WSSideState(
                        order_id=int(oid),
                        nonce=None,
                        price=price,
                        qty=qty,
                        side=side,
                        placed_at=time.time()
                    )
                    if state_key == "bid":
                        self.state.bid = new_state
                    else:
                        self.state.ask = new_state
                    log.warning("[%s] REST fallback PLACE %s @ %.2f x %.6f -> oid=%s",
                                self.symbol, side, price, qty, oid)
            except Exception as re:
                log.error("[%s] REST fallback place %s failed: %s", self.symbol, side, re)

    async def _ws_cancel_side(self, state_key: str) -> bool:
        """Cancel one side's order via WS/REST. Returns True if cancel succeeded."""
        st = self.state.bid if state_key == "bid" else self.state.ask
        if not st.order_id:
            return True

        cancelled = False
        try:
            await self.ws.cancel_order(
                order_id=st.order_id,
                nonce=st.nonce or 0
            )
            log.debug("[%s] WS CANCEL %s oid=%s",
                      self.symbol, state_key.upper(), st.order_id)
            cancelled = True
        except Exception as e:
            log.debug("[%s] WS cancel %s failed: %s", self.symbol, state_key, e)
            try:
                res = await asyncio.to_thread(self.rest.cancel_order, self.symbol, str(st.order_id), None)
                if isinstance(res, dict) and res.get("status") == "error":
                    log.debug("[%s] REST fallback CANCEL %s returned error: %s",
                              self.symbol, state_key.upper(), res)
                else:
                    log.debug("[%s] REST fallback CANCEL %s oid=%s",
                              self.symbol, state_key.upper(), st.order_id)
                    cancelled = True
            except Exception as re:
                log.debug("[%s] REST fallback cancel %s failed: %s", self.symbol, state_key, re)

        # Final verification against exchange state.
        try:
            orders = await self._get_symbol_open_orders()
            order_still_live = any(o["id"] == int(st.order_id) for o in orders)
            if cancelled and order_still_live:
                # Cancel API said OK but order is still on exchange.
                cancelled = False
                log.warning("[%s] Cancel not confirmed for %s oid=%s",
                            self.symbol, state_key.upper(), st.order_id)
            elif not cancelled and not order_still_live:
                # Cancel API failed (e.g. order already filled/cancelled by exchange)
                # but order is genuinely gone — treat as successfully cancelled.
                cancelled = True
                log.info("[%s] %s order %s already gone (filled/expired), treating as cancelled",
                         self.symbol, state_key.upper(), st.order_id)
        except Exception as e:
            log.debug("[%s] Cancel verification failed: %s", self.symbol, e)

        if cancelled:
            if state_key == "bid":
                self.state.bid = WSSideState()
            else:
                self.state.ask = WSSideState()

        return cancelled

    def request_shutdown(self):
        self.shutdown = True
