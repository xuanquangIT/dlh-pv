from __future__ import annotations
from typing import Iterable, List, Optional
from pv_lakehouse.etl.clients import openelectricity
from pv_lakehouse.etl.clients.openmeteo import FacilityLocation
from pv_lakehouse.etl.utils.parsing import parse_csv_list

def resolve_facility_codes(
    facility_codes: Optional[str] = None,
    *,
    uppercase: bool = True,
) -> List[str]:
    """Resolve facility codes from CLI input or fall back to OpenElectricity defaults.

    This function handles the common pattern of:
    1. Parsing comma-separated facility codes from CLI --facility-codes argument
    2. Normalizing codes to uppercase (optional)
    3. Falling back to default solar facility codes if none provided

    Args:
        facility_codes: Comma-separated facility codes string, or None.
        uppercase: Whether to convert codes to uppercase (default: True).

    Returns:
        List of facility codes, either from input or defaults.

    Examples:
        >>> resolve_facility_codes("YARRANL1, GULLRWF1")
        ['YARRANL1', 'GULLRWF1']
        >>> resolve_facility_codes(None)  # Returns default solar facilities
        ['YARRANL1', 'GULLRWF1', ...]  # Default codes from OpenElectricity
        >>> resolve_facility_codes("abc, def", uppercase=True)
        ['ABC', 'DEF']
    """
    codes = parse_csv_list(facility_codes)
    if codes:
        if uppercase:
            return [code.upper() for code in codes]
        return codes
    return openelectricity.load_default_facility_codes()


def load_facility_locations(
    facility_codes: Iterable[str],
    api_key: Optional[str] = None,
    *,
    networks: Optional[List[str]] = None,
    statuses: Optional[List[str]] = None,
    fueltechs: Optional[List[str]] = None,
) -> List[FacilityLocation]:
    """Load facility metadata with coordinates for Open-Meteo API calls.

    Fetches facility metadata from OpenElectricity API and returns
    FacilityLocation objects with lat/lng coordinates.

    Args:
        facility_codes: Iterable of facility codes to load.
        api_key: Optional override API key.
        networks: Network filter (default: ["NEM", "WEM"]).
        statuses: Status filter (default: ["operating"]).
        fueltechs: Fuel tech filter (default: ["solar_utility"]).

    Returns:
        List of FacilityLocation objects with coordinates.

    Raises:
        ValueError: If no facilities returned or missing coordinates.

    Examples:
        >>> locations = load_facility_locations(["YARRANL1", "GULLRWF1"])
        >>> for loc in locations:
        ...     print(f"{loc.code}: {loc.latitude}, {loc.longitude}")
    """
    networks = networks or ["NEM", "WEM"]
    statuses = statuses or ["operating"]
    fueltechs = fueltechs or ["solar_utility"]

    selected_codes = [code.strip().upper() for code in facility_codes if code.strip()]
    facilities_df = openelectricity.fetch_facilities_dataframe(
        api_key=api_key,
        selected_codes=selected_codes or None,
        networks=networks,
        statuses=statuses,
        fueltechs=fueltechs,
    )

    if facilities_df.empty:
        raise ValueError("No facilities returned from OpenElectricity metadata API")

    facilities_df = facilities_df.dropna(subset=["location_lat", "location_lng"])
    if selected_codes:
        facilities_df = facilities_df[
            facilities_df["facility_code"].str.upper().isin(selected_codes)
        ]

    if facilities_df.empty:
        raise ValueError("Requested facilities missing latitude/longitude data")

    facilities: List[FacilityLocation] = []
    for row in facilities_df.itertuples(index=False):
        facilities.append(
            FacilityLocation(
                code=str(row.facility_code),
                name=str(row.facility_name or row.facility_code),
                latitude=float(row.location_lat),
                longitude=float(row.location_lng),
            )
        )
    return facilities


__all__ = [
    "resolve_facility_codes",
    "load_facility_locations",
]
