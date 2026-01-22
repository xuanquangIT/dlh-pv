from __future__ import annotations
import datetime as dt
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from .models import (
    Facility,
    FacilityMetadata,
    FacilitySummary,
    TimeseriesRow,
)

NETWORK_TIMEZONE_IDS = {"NEM": "Australia/Brisbane", "WEM": "Australia/Perth"}
NETWORK_FALLBACK_OFFSETS = {"NEM": 10, "WEM": 8}
MAX_RANGE_ERROR_PATTERN = re.compile(r"Maximum range is (\d+) days", re.IGNORECASE)

