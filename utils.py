from __future__ import annotations
import time
from dataclasses import dataclass
from decimal import Decimal, getcontext

getcontext().prec = 50


def now_ms() -> int:
    return int(time.time() * 1000)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def bps_to_price(px: float, bps: float) -> float:
    return px * (bps / 10000.0)


def pct_of(x: float, p: float) -> float:
    return x * (p / 100.0)


def get_precision(value: float) -> int:
    """Get number of decimal places for a float value"""
    if value == 0:
        return 0
    s = f"{value:.10f}".rstrip('0').rstrip('.')
    if '.' in s:
        return len(s.split('.')[1])
    return 0


@dataclass
class ContractSpec:
    symbol: str
    tick_size: float = 0.01
    step_size: float = 0.001
    min_qty: float = 0.001
    min_notional: float = 10.0
    contract_size: float = 1.0

    def q_price(self, price: float) -> float:
        """Quantize price to tick_size (rounds down)"""
        v = Decimal(str(price))
        s = Decimal(str(self.tick_size))
        return float((v // s) * s)

    def q_price_floor(self, price: float) -> float:
        """Round price DOWN to tick_size (for bids)"""
        return self.q_price(price)

    def q_price_ceil(self, price: float) -> float:
        """Round price UP to tick_size (for asks)"""
        v = Decimal(str(price))
        s = Decimal(str(self.tick_size))
        result = ((v // s) + 1) * s
        if v % s == 0:
            return float(v)
        return float(result)

    def q_qty(self, qty: float) -> float:
        """Quantize quantity to step_size (rounds down)"""
        v = Decimal(str(qty))
        s = Decimal(str(self.step_size))
        return float((v // s) * s)


class ATR:
    def __init__(self, length: int):
        self.len = length
        self.rma = None
        self.prev_close = None
        self.alpha = 1.0 / length

    def _tr(self, h, l, c_prev):
        if c_prev is None:
            return float(h - l)
        return float(max(h - l, abs(h - c_prev), abs(l - c_prev)))

    def update_bar(self, o, h, l, c, closed: bool):
        tr = self._tr(h, l, self.prev_close)
        if self.rma is None:
            self.rma = tr
        else:
            self.rma = (1 - self.alpha) * self.rma + self.alpha * tr
        if closed:
            self.prev_close = c
        return self.rma, tr