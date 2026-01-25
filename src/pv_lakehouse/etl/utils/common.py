from __future__ import annotations
from typing import List, Optional

def parse_csv(value: Optional[str]) -> List[str]:
    # parse csv từ string thành list
    return [item.strip() for item in (value or "").split(",") if item.strip()]

__all__ = ["parse_csv"]
