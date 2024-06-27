import json

from dotenv import dotenv_values
from http import HTTPStatus

import requests

FLASK_ENV_VARS = dotenv_values(".flaskenv")


def ping():
    url = f"http://localhost:{FLASK_ENV_VARS['FLASK_RUN_PORT']}/ping"
    print(f"Pinging at url {repr(url)}")
    r = requests.get(url=url)
    if r.status_code != HTTPStatus.OK:
        raise requests.HTTPError(r.content.decode(), r.status_code)
    response_data = json.loads(r.content)
    print(response_data)


def execute_test_process():
    url = f"http://localhost:{FLASK_ENV_VARS['FLASK_RUN_PORT']}/processes/test/execution"
    print(f"Executing test process at url {repr(url)}")
    r = requests.post(url=url, json={})
    if r.status_code != HTTPStatus.OK:
        raise requests.HTTPError(r.content.decode(), r.status_code)
    response_data = json.loads(r.content)
    print(response_data)


def main():
    ping()
    execute_test_process()


if __name__ == "__main__":
    main()
