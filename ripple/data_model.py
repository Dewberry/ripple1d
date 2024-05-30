from dataclasses import dataclass


@dataclass
class FlowChangeLocation:
    """
    HEC-RAS Flow Change Locations
    """

    river: str = None
    reach: str = None
    rs: float = None
    rs_str: str = None
    flows: list[float] = None


@dataclass
class XS:
    """
    HEC-RAS 1D XS
    """

    river: str = None
    reach: str = None
    river_reach: str = None
    rs: float = None

    left_reach_length: float = None
    channel_reach_length: float = None
    right_reach_length: float = None
    rs_str: str = None
    river_reach_rs: str = None

    description: str = None
    number_of_coords: int = None
    number_of_station_elevation_points: int = None
    coords: list[float] = None
    station_elevation: list[float] = None
    thalweg: float = None
    max_depth: float = None
    mannings: list[float] = None
    left_bank: float = None
    right_bank: float = None
