# ripple
[![CI](https://github.com/dewberry/ripple/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/dewberry/ripple/actions/workflows/ci.yaml)

Utilities for repurposing HEC-RAS models for use in the production of Flood Inundation Maps (FIMs) and rating curves for use in near-real time flood forecasting on the NOAA National Water Model network.

## Contents

 - [api](api/) : Source code for a [Flask](https://flask.palletsprojects.com/en/3.0.x/) API and [Huey](https://huey.readthedocs.io/en/latest/) queueing system for managing parallel compute. 
 - [production](production/) : This directory contains scripts used by the development team for testing ripple outside of the API. The contents may not be stable and will often not be up to date.
 - [ripple](ripple/): Source code for the ripple library.
 - [tests](tests/): Unit tests.

## Requirements

*OS Dependency*: Ripple requires a Windows environment with Desktop Experience (GUI, not a headless Windows server) and [HEC-RAS](https://www.hec.usace.army.mil/software/hec-ras/download.aspx) installed (currently version 6.3.1 is supported).

Ripple requires Python version >=3.10 on a Windows host. 

## Usage

When successfully installed, a standalone executable will be available (stored in the path) allowing you to manage the Flask API and Huey consumer direcly by calling `ripple` in either a Command Prompt or PowerShell termiak. Below are the steps to start, stop, and check the status of the Ripple Manager..

1. **Start the Ripple Services**:
    ```powershell
    ripple start  --flask_port 5000 --thread_count 5 
    ```
1. **Check the status**:
    ```powershell
    ripple status --pids_file ./process-ids.json
    ```
1. **Stop the Ripple Services**:
    ```powershell
    python ripple_manager.py stop --pids_file ./process-ids.json
    ```



For a full test of the REST API see the [REST API documentation](api/README.md).

## Installing Ripple

**NOTE: Using a python virtual environment is not required but is highly recommended.**

### Example setup

1. Install [Python](https://www.python.org/downloads/)
2. Create a virtual Python environment using Option 1 or 2:
    
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

3. Activate (enter) the new virtual environment using Option 1 or 2:

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

5. Install the `ripple` package using `pip`:
    ```powershell
    pip install ripple
    ```

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

