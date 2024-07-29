import sqlite3
from typing import List, Optional, Tuple

# Paths
conflation_db_path = r"D:\Users\abdul.siddiqui\workbench\projects\production\library.sqlite"

conn = sqlite3.connect(conflation_db_path)
cursor = conn.cursor()


def get_reaches_with_valid_data() -> List[Tuple[int, int]]:
    """Get reaches that have valid model_key and us_xs_id not equal to -9999.0."""
    cursor.execute(
        """
        SELECT reach_id, nwm_to_id FROM conflation
        WHERE model_key IS NOT NULL AND us_xs_id != -9999.0
    """
    )
    return cursor.fetchall()


def get_leap_reaches() -> List[Tuple[int, int]]:
    """Get reaches that have valid model_key but us_xs_id equal to -9999.0."""
    cursor.execute(
        """
        SELECT reach_id, nwm_to_id FROM conflation
        WHERE model_key IS NOT NULL AND us_xs_id = -9999.0
    """
    )
    return cursor.fetchall()


def update_conflation_to_id(reach_id: int, conflation_to_id: int) -> None:
    """Update the conflation_to_id for a given reach."""
    cursor.execute(
        """
        UPDATE conflation
        SET conflation_to_id = ?
        WHERE reach_id = ?
    """,
        (conflation_to_id, reach_id),
    )
    conn.commit()


def build_modified_network():
    """Build the modified network by updating conflation_to_id based on the given logic."""
    valid_reaches = get_reaches_with_valid_data()
    leap_reaches = get_leap_reaches()
    valid_reaches_dict = {reach_id: nwm_to_id for reach_id, nwm_to_id in valid_reaches}
    leap_reaches_dict = {reach_id: nwm_to_id for reach_id, nwm_to_id in leap_reaches}

    for reach_id, nwm_to_id in valid_reaches:
        current_reach_id = nwm_to_id
        while current_reach_id:  # current_reach_id can become Null when we reach the ocean
            if current_reach_id in valid_reaches_dict:
                # Found a valid reach, update conflation_to_id and break the loop
                update_conflation_to_id(reach_id, current_reach_id)
                break
            elif current_reach_id in leap_reaches_dict:
                # Current reach is a leap reach, continue to follow the nwm_to_id
                current_reach_id = leap_reaches_dict[current_reach_id]
            else:
                # Reach is not in valid_reaches_dict or leap_reaches_dict, break the loop
                break


build_modified_network()
conn.close()
