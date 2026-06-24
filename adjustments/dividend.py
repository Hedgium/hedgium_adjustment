"""
Forward-subtraction dividend adjustment (mirrors backend optionchain/dividend_spot.py).
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

DAILY_CARRY_RATE = 0.0001644


def _parse_ex_date(dividend: dict[str, Any]) -> Optional[date]:
    raw = dividend.get("ex_dividend_date")
    if not raw:
        return None
    if isinstance(raw, date):
        return raw
    try:
        return date.fromisoformat(str(raw))
    except (TypeError, ValueError):
        return None


def effective_spot_for_greeks(
    spot: float,
    option_expiry: date,
    dividend: Optional[dict[str, Any]],
    *,
    today: Optional[date] = None,
) -> float:
    S = float(spot or 0)
    if S <= 0:
        return S

    if not dividend or not dividend.get("active"):
        return S

    amount = dividend.get("amount")
    if amount is None or float(amount) <= 0:
        return S

    ex_date = _parse_ex_date(dividend)
    if ex_date is None:
        return S

    ref_day = today or date.today()
    if ref_day >= ex_date:
        return S
    if option_expiry <= ex_date:
        return S

    days = max((option_expiry - ref_day).days, 0)
    return S * (1.0 + days * DAILY_CARRY_RATE) - float(amount)
