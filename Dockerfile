FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

RUN apt-get update && \
    apt-get install -y python3-pip  python3-venv

WORKDIR /app

COPY ripple1d ripple1d
COPY pyproject.toml .
COPY MANIFEST.in .

RUN python3 -m venv venv
RUN venv/bin/pip install build && \
    venv/bin/python -m build && \
    venv/bin/pip install .[dependencies] && \
    venv/bin/pip install pystac_client pystac papipyplug

COPY ripple1d/ops/stac_item.py .