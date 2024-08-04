# ripple
[![CI](https://github.com/dewberry/ripple/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/dewberry/ripple/actions/workflows/ci.yaml)


Utilities for repurposing HEC-RAS models for use in the production of Flood Inundation Maps (FIMs) and rating curves for use in near-real time flood forecasting on the NOAA National Water Model network.

## Contents

 - [api](api/) : Source code for a Flask API and Huey queueing system for managing parallel compute. 
 - [production](production/) : Stand alone scripts providing examples of using the ripple library without the API.
 - [ripple](ripple/): Source code for the ripple library.
 - [tests](tests/): Unit tests.

## Getting Started

*OS Dependency*: Ripple requires a Windows environment with Desktop Experience (GUI, not a headless Windows server) and [HEC-RAS](https://www.hec.usace.army.mil/software/hec-ras/download.aspx) installed (currently version 6.3.1 is supported).


These steps assume you will be using Python version 3.12 on a Windows host. Alternate versions of Python can typically be used by replacing "312" in the below steps, e.g. use "311" for Python 3.11.

All commands should be ran within a standard Terminal (not PowerShell)

### Python Virtual Environment
Using a python virtual environment is highly recommended. 

#### Example setup
1. Install [Python](https://www.python.org/downloads/)
1. Create a virtual Python environment: 
    ```bat
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" -m venv "%homepath%\venvs\ripple-py312"
    ```
1. Activate (enter) the new virtual environment: 
    ```bat 
    "%homepath%\venvs\ripple-py312\Scripts\activate.bat"
    ```
1. Confirm that the activation worked.
    1. You should see that a parenthetical `(ripple-py312)` has been added to the left side of your current line in the terminal
    1. Enter `where python` and confirm that the top result is sourcing Python from the new directory

### Installing Dependencies

1. Activate (enter) the new virtual environment (if not already): 
    ```bat
    "%homepath%\venvs\ripple-py312\Scripts\activate.bat"
    ```
1. Update pip: `python -m pip install --upgrade pip`
1. Change directory into the root of this repository: `cd C:\path\to\this\repo`
1. Install dependencies from [pyproject.toml](pyproject.toml). In this example we would like to include the optional "dev" dependency group, in addition to the required dependencies: `python -m pip install ".[dev]"`

### Configuring Ripple Environment Variables

1. Copy [.env.example](.env.example) and rename the copy to `.env`
1. Edit `.env` as necessary to specify the path to the virtual Python environment, to specify the number of huey threads, etc. `.env` must at least contain the variables `VENV_PATH` and `HUEY_THREAD_COUNT`.

### Testing the Installation

For a full test of the REST API see the [REST API documentation](api/README.md).
