"""FX-pair leg identity: parsing a series_code's two currency legs, and the fixed leg/anchor-
currency vocabulary every AvailabilitySpec-driven report uses.

base/quote/USD are generic to "a currency pair", not specific to any one availability spec's
roles -- unlike ROLE_FILTERS/REQUIRED_ROLES (STEER's answers, supplied via AvailabilitySpec),
there's nothing for a caller to configure here.
"""

from __future__ import annotations

import re
from typing import Literal, Optional, Tuple

LEG_BASE: Literal["base"] = "base"
LEG_QUOTE: Literal["quote"] = "quote"
LEGS: Tuple[str, str] = (LEG_BASE, LEG_QUOTE)

#: The anchor currency every "single non-USD leg" rule checks against (see
#: AvailabilitySpec.single_non_usd_leg / report.py's _non_usd_leg).
CURRENCY_USD: Literal["USD"] = "USD"

_FX_PAIR_PATTERN = re.compile(r"^([A-Z]{3})([A-Z]{3})_")


def parse_fx_legs(series_code: str) -> Optional[Tuple[str, str]]:
    """Extract (base, quote) ISO currency codes from an FX series_code like "EURUSD_PX_LAST"."""
    match = _FX_PAIR_PATTERN.match(str(series_code))
    return (match.group(1), match.group(2)) if match else None


def non_usd_leg(base: str, quote: str) -> Optional[str]:
    """Whichever of base/quote isn't USD -- None if neither (or both) are USD."""
    if base == CURRENCY_USD and quote != CURRENCY_USD:
        return quote
    if quote == CURRENCY_USD and base != CURRENCY_USD:
        return base
    return None
