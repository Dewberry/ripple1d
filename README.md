# ripple
Utilities for reuse of HEC-RAS models for NWM. HEC-RAS models must be cataloged in a Spatial Temporal Asset Catalog (STAC) and conflated with the NWM reach dataset prior to processing through ripple. 

## Contents
Refactor in progress

## Getting Started

Some parts of ripple require a Windows environment. Furthermore, some parts require that the Windows environment be a Desktop Experience (GUI, not a headless Windows server).

These steps assume you will be using Python version 3.12 on a Windows host. Alternate versions of Python can typically be used by replacing "312" in the below steps, e.g. use "311" for Python 3.11.

All commands should be ran within a standard Terminal (not PowerShell)

### Initializing the Python Virtual Environment

1. Install [Python](https://www.python.org/downloads/)
1. Create a virtual Python environment: `"%LOCALAPPDATA%\Programs\Python\Python312\python.exe" -m venv "%homepath%\venvs\ripple-py312"`
1. Activate (enter) the new virtual environment: `"%homepath%\venvs\ripple-py312\Scripts\activate.bat"`
1. Confirm that the activation worked.
    1. You should see that a parenthetical "(ripple-py312)" has been added to the left side of your current line in the terminal
    1. Enter `where python` and confirm that the top result is sourcing Python from the new directory

### Installing Dependencies

1. Activate (enter) the new virtual environment (if not already): `"%homepath%\venvs\ripple-py312\Scripts\activate.bat"`
1. Update pip: `python -m pip install --upgrade pip`
1. Change directory into the root of this repository: `cd C:\path\to\this\repo`
1. Install dependencies from [pyproject.toml](pyproject.toml). In this example we would like to include the optional "dev" dependency group, in addition to the required dependencies: `python -m pip install ".[dev]"`

### Configuring Ripple Environment Variables

1. Copy [.env.example](.env.example) and rename the copy to `.env`
1. Edit `.env` as necessary to specify the path to the virtual Python environment, to specify the number of huey threads, etc. `.env` must at least contain the variables `VENV_PATH` and `HUEY_THREAD_COUNT`.

### Testing the Installation

For a full test of the REST API, see the REST API section below.


## About
Producing inundation maps at half-foot increments for each NWM branch in a given RAS model is a multi-step process outlined below. "run_process.py" is a script that executes the process in the necessary sequential order. 

1. Read the input STAC item
2. Download HEC-RAS files
3. Load NWM conflation parameters.
2. Determine flow increments for each NWM branch in the RAS model using upper and lower flow ranges specified in conflation parameters. Default is 10 increments.
3. For each NWM branch, write a flow and plan file for an initial run using the incremented flows. 
4. For each NWM branch, Use the results from the initial run to develop rating curves at the upstream and downstream terminus of each NWM branch. 
5. For the upstream rating-curve of each branch, determine what flows would be needed to produce half-foot increments.
6. For the downstream rating-curve of each branch, determine depths at half-foot increments over the range of flows applied to the branch. 
7. Convert the downstream depths to water surface elevations
8. For each NWM branch create a production-run flow/plan file. 
    - The discharges derived from the upstream rating-curve are applied at the top of the HEC-RAS river-reach
    - The water surface elevations derived from the downstream rating-curve are applied as an intermediate known water surface elevation at the downstream terminus of the NWM branch
9. Run the production-runs with post-processing depth rasters toggled on
10. For each NWM branch, clip each resulting depth raster to a concave hull derived from the cross sections associated with each branch.
11. For each NWM branch read the HDF results and write rating-curves to sqlite db.  

<br>
<br>

# REST API

## About the API

The REST API server leverages huey for concurrent task (job) execution, and Flask as the user HTTP REST interface into the huey
system. The HTTP endpoints adhere closely to [OGC Process API](https://ogcapi.ogc.org/processes/overview.html) standards.

## Environment Requirements of the API

1. Windows host with Desktop Experience, due to its usage of the HEC-RAS GUI.
1. A virtual Python environment with dependencies installed per [pyproject.toml](pyproject.toml).
1. HEC-RAS installed, and EULA accepted. For each version of RAS, open the program once to read and accept the EULA.

## API Launch Steps

1. Initialize or edit `.env` as necessary to specify the virtual Python environment to use, the number of huey threads to use, data access credentials, etc. Care should be taken not to include the same variable names in [.flaskenv](.flaskenv) and in `.env`.
1. If necessary, edit [.flaskenv](.flaskenv) (do not store any secrets or credentials in this file!)
1. Double-click [api-start.bat](api-start.bat). This will cause two Windows terminals to open. One will be running huey and the other will be running Flask. **Warning: this will delete and re-initialize the `api\logs\` directory, which includes the huey tasks database in addition to the log files.**
1. Double-click [api-test.bat](api-test.bat). This will send some requests to the API confirming that it is online and ready to process jobs.

## API Administration Notes

**Warning: [api-start.bat](api-start.bat) will delete and re-initialize the `api\logs\` directory, which includes the huey tasks database in addition to the log files.**

huey is configured to use a local SQLite database as its store for managing tasks and storing their returned values. If the db file
does not exist, it will be created when the huey consumer is executed. If it does exist, it will be used as-is and not overridden.
Therefore if the server administrator needs to ungracefully stop all tasks and/or re-start the server, then if they want to be sure that
any existing tasks are fully cleared / removed from the system, they should manually delete the db file themselves before re-starting
the server.
