# API

The REST API server leverages huey for concurrent task (job) execution, and Flask as the user HTTP REST interface into the huey
system. The HTTP endpoints adhere closely to [OGC Process API](https://ogcapi.ogc.org/processes/overview.html) standards.

## Environment Requirements of the API

1. Windows host with Desktop Experience, due to its usage of the HEC-RAS GUI.
1. A virtual Python environment with dependencies installed per [pyproject.toml](../pyproject.toml).
1. HEC-RAS installed, and EULA accepted. For each version of RAS, open the program once to read and accept the EULA.

## API Launch Steps

1. Initialize or edit `.env` as necessary to specify the virtual Python environment to use, the number of huey threads to use, data access credentials, etc. Care should be taken not to include the same variable names in [.flaskenv](../.flaskenv) and in `.env`.
1. If necessary, edit [.flaskenv](../.flaskenv) (do not store any secrets or credentials in this file!)
1. Double-click [api-start.bat](../api-start.bat). This will cause two Windows terminals to open. One will be running huey and the other will be running Flask. **Warning: this will delete and re-initialize the `api\logs\` directory, which includes the huey tasks database in addition to the log files.**
1. Double-click [api-test.bat](../api-test.bat). This will send some requests to the API confirming that it is online and ready to process jobs.

## API Administration Notes

**Warning: [api-start.bat](../api-start.bat) will delete and re-initialize the `api\logs\` directory, which includes the huey tasks database in addition to the log files.**

**huey** is configured to use a local SQLite database as its store for managing tasks and storing their returned values. If the db file
does not exist, it will be created when the huey consumer is executed. If it does exist, it will be used as-is and not overridden.
Therefore if the server administrator needs to ungracefully stop all tasks and/or re-start the server, then if they want to be sure that
any existing tasks are fully cleared / removed from the system, they should manually delete the db file themselves before re-starting
the server.
