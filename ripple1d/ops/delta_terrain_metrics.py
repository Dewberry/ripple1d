import numpy as np


def terrain_bias(src_el: np.ndarray, dem_el: np.ndarray) -> np.ndarray:
    """Calculate the average terrain difference between two elevation profiles."""
    all_stations = np.sort(np.unique(np.concatenate((src_el[:, 0], dem_el[:, 0]))))
    src_el = np.interp(all_stations, src_el[:, 0], src_el[:, 1])
    dem_el = np.interp(all_stations, dem_el[:, 0], dem_el[:, 1])
    return np.trapezoid(src_el - dem_el, all_stations) / (all_stations[-1] - all_stations[0])


def series_pct_diff(s1: np.ndarray, s2: np.ndarray, wse: float) -> float:
    """Calculate the percent difference between two curves at a water surface elevation."""
    s1 = np.interp(wse, s1[:, 0], s1[:, 1])
    s2 = np.interp(wse, s2[:, 0], s2[:, 1])
    return ((s1 - s2) / s1) * 100


def pct_incorrect_inundation(src_el: np.ndarray, dem_el: np.ndarray, wse: float) -> float:
    """Calculate the percent of the inundated area that is correct."""
    all_stations = np.sort(np.unique(np.concatenate((src_el[:, 0], dem_el[:, 0]))))
    src_el = np.interp(all_stations, src_el[:, 0], src_el[:, 1])
    dem_el = np.interp(all_stations, dem_el[:, 0], dem_el[:, 1])
    src_wet = src_el < wse
    dem_wet = dem_el < wse
    matching = (src_wet != dem_wet) * 1
    return (np.trapezoid(matching, all_stations) / (all_stations[-1] - all_stations[0])) * 100
