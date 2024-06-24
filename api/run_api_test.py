import json

from dotenv import dotenv_values
from http import HTTPStatus

import requests

DOTENV_VALS = dotenv_values(".flaskenv")


def main():
    port = DOTENV_VALS["FLASK_RUN_PORT"]
    url = f"http://localhost:{port}/ping"

    print(f"Pinging flask API at url {repr(url)}")
    r = requests.get(url=url, json={})
    if r.status_code != HTTPStatus.OK:
        raise requests.HTTPError(r.content.decode(), r.status_code)

    response_data = json.loads(r.content)
    print(response_data)


if __name__ == "__main__":
    main()
