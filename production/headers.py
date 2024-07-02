"""Manage Headers."""

import json
import os

import requests


def get_auth_header():
    """Get auth header for a given user."""
    auth_server = os.getenv("AUTH_ISSUER")
    client_id = os.getenv("AUTH_ID")
    client_secret = os.getenv("AUTH_SECRET")

    username = os.getenv("AUTH_USER")
    password = os.getenv("AUTH_USER_PASSWORD")

    auth_payload = f"username={username}&password={password}&client_id={client_id}&grant_type=password&client_secret={client_secret}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Bearer null",
    }

    auth_response = requests.request("POST", auth_server, headers=headers, data=auth_payload)

    try:
        token = json.loads(auth_response.text)["access_token"]
    except KeyError:
        logging.debug(auth_response.text)
        raise KeyError

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-ProcessAPI-User-Email": username,
    }

    return headers
