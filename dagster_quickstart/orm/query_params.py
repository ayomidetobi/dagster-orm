"""Parameter objects for query methods."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ValueQueryParams:
    """Parameters for value queries.

    Attributes:
        start: Start timestamp (inclusive). Format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
        end: End timestamp (inclusive). Format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
        limit: Maximum number of rows to return
        order_by: Column name to order by (default: timestamp)
    """

    start: Optional[str] = None
    end: Optional[str] = None
    limit: Optional[int] = None
    order_by: Optional[str] = None
