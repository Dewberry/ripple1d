# ripple
[![CI](https://github.com/dewberry/ripple/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/dewberry/ripple/actions/workflows/ci.yaml)
[![Documentation Status](https://readthedocs.org/projects/ripple/badge/?version=latest)](https://ripple.readthedocs.io/en/latest/?badge=latest)
[![Release](https://github.com/dewberry/ripple/actions/workflows/release.yaml/badge.svg)](https://github.com/dewberry/ripple/actions/workflows/release.yaml)
[![PyPI version](https://badge.fury.io/py/ripple.svg)](https://badge.fury.io/py/ripple)

Utilities for repurposing HEC-RAS models for use in the production of Flood Inundation Maps (FIMs) and rating curves for use in near-real time flood forecasting on the NOAA National Water Model network. Go to [ReadTheDocs](http://ripple.readthedocs.io/) for more information on ripple.

## Contents

 - [api](api/) : Source code for the [Flask](https://flask.palletsprojects.com/en/3.0.x/) API and [Huey](https://huey.readthedocs.io/en/latest/) queueing system for managing parallel compute. 
 - [production](production/) (*Deprecation Warning*) : This directory contains scripts used by the development team for testing ripple outside of the API. The contents are not included in the PyPi package and may not be stable or up to date. 
 - [ripple](ripple/): Source code for the ripple library.
 - [tests](tests/): Unit tests.up


## Requirements

*OS Dependency*: Ripple requires Python version >=3.10 and a Windows environment with Desktop Experience (GUI, not a headless Windows server) and [HEC-RAS](https://www.hec.usace.army.mil/software/hec-ras/download.aspx) installed (currently version 6.3.1 is supported).


## Installing Ripple

##### *NOTE: Using a python virtual environment is not required but is highly recommended.*

### Using pip

1. Activate virtual environment as shown below and install the `ripple` package using `pip`:

    ```powershell
    pip install ripple
    ```

### Building from source 

1. Activate virtual environment as shown below and install the `ripple` package using `pip`:

    ```powershell
    pip install ripple
    ```

    ---

### Testing the Installation

1. Verify the installation by importing `ripple` in a Python shell:
    ```powershell
    python
    >>> import ripple
    >>> print(ripple.__version__)
    ```

2. Run the unit tests to ensure everything is working correctly:
    ```powershell
    pytest tests/
    ```

See the [REST API documentation](docs/api/README.rst) for available endpoints and usage.

### Setting up a virtual environment

1. Install [Python](https://www.python.org/downloads/)
2. Create a virtual Python environment using Command Prompt or PowerShell:
    
    *Option 1:* Windows Command Prompt
    ```bat
    %LOCALAPPDATA%\Programs\Python\Python312\python.exe -m venv %homepath%\venvs\venv-py312
    ```

    *Option 2:* Windows PowerShell
    ```powershell
    $pythonExe = "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe"
    $venvPath = "$env:USERPROFILE\venvs\venv-py312"
    & $pythonExe -m venv $venvPath
    ```

3. Activate (enter) the new virtual environment:

    *Option 1:* Windows Command Prompt
    ```bat
    %homepath%\venvs\venv-py312\Scripts\activate.bat
    ```

    *Option 2:* Windows PowerShell
    ```powershell
    & "$env:USERPROFILE\venvs\venv-py312\Scripts\Activate.ps1"
    ```

4. Confirm that the activation worked.
    1. You should see that a parenthetical `(venv-py312)` has been added to the left side of your current line in the terminal.
    2. Enter `where python` and confirm that the top result is sourcing Python from the new directory.

---


### Credits and References
1. [Office of Water Prediction (OWP)](https://water.noaa.gov/)
1. [Dewberry](https://www.dewberry.com/)
1. [Raytheon](https://www.rtx.com/)
1. [ Earth Resources Technology, Inc.](https://www.ertcorp.com/)
1. [ras2fim](https://github.com/NOAA-OWP/ras2fim)
1. [USACE HEC-RAS](https://www.hec.usace.army.mil/software/hec-ras/)
1. NOAA National Water Model [(NWM)](https://water.noaa.gov/about/nwm)




**Special Thanks to:** David Bascom (FEMA), Paul Rooney (FEMA),  Julia Signell and Dan Pilone of [Element84](https://www.element84.com/), and the developers of [STAC](https://stacspec.org/en).