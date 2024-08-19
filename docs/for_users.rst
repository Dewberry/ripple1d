User Guide / Manual
====================

This section provides a high level overview for using ripple1d for production. 

.. note::
   ripple1d is in active development and changes will be made frequently as production activities test the crurrent design and features.


Getting Started
----------------


:code:`ripple1d` is registered with `PyPI <https://pypi.org/project/ripple1d>`_ and can be installed simply using python's pip package installer. 
Assuming you have Python already installed and setup:

   .. code-block:: powershell

      pip install ripple1d


Note that it is highly recommended to create a python `virtual environment <https://docs.python.org/3/library/venv.html>`_ to install, test, and run ripple. 

When successfully installed, a standalone executable will be available (stored in the path) allowing you to manage the 
Flask API and Huey consumer direcly by calling ``ripple1d`` in either a Command Prompt or PowerShell terminal
Below are the steps to start the Ripple Manager, including the `thread_count` option for allocating the
number of cpu's to dedicate to ripple jobs..


**Start the Ripple Services**:

   .. code-block:: powershell

      ripple1d start --thread_count 5 


**Help for the Ripple Services**:

   .. code-block:: powershell

      ripple1d -h

      ripple1d start -h


By default, starting ripple1d will launch 2 terminal windows, one for the Flask API and the other for the Huey consumer. Logs for each of
these services are stored in the same directory where ripple1d was started. For example, if you started ripple1d in the directory 
``C:\Users\user\Desktop``, the you will see 2 new files appear on the Desktop:


    **Server log**: 2024-08-18T18-46-42.828592+00-00-ripple1d-flask.jsonld
     
    **Jobs log**: 2024-08-18T18-46-42.376085+00-00-ripple1d-huey.jsonld`

Verify that ripple is running by checking the status here: http://localhost/ping

.. code-block:: JSON

   {"status" : "healthy"}

A healthy status indicates that ripple1d is running and waiting for jobs. 


Submitting Jobs
----------------
Jobs can be submitted using Postman collections, python clients, curl, or any other software that can communicate via HTTP REST protocol.


Monitoring Logs
----------------
Logs are provided in json-ld (linked data) format. The logs are written to file youing next line delimiting so the files can be read in and converted easily for evaluation.

An example of the huey (Jobs) log is shown here:

.. code-block:: JSON

   {"@type": "RippleLogs", "timestamp": "2024-08-18T18:46:42Z", "level": "INFO", "msg": "Huey consumer started with 4 thread, PID 5068 at 2024-08-18 18:46:42.444990"}
   {"@type": "RippleLogs", "timestamp": "2024-08-18T18:46:42Z", "level": "INFO", "msg": "Scheduler runs every 1 second(s)."}
   {"@type": "RippleLogs", "timestamp": "2024-08-18T18:46:42Z", "level": "INFO", "msg": "Periodic tasks are enabled."}
   {"@type": "RippleLogs", "timestamp": "2024-08-18T18:46:42Z", "level": "INFO", "msg": "The following commands are available:\n+ ripple1d.api.tasks._process"}
   {"@type": "RippleLogs", "timestamp": "2024-08-18T18:47:41Z", "level": "INFO", "msg": "Executing ripple1d.api.tasks._process: cc6cf9f2-ab0a-4a36-90a2-81e80157a907"}
   {"@type": "RippleLogs", "timestamp": "2024-08-18T18:48:18Z", "level": "INFO", "msg": "Executing ripple1d.api.tasks._process: 19e04b34-ebf8-421e-850c-af8adff09728"}

An example of the flask (server) logs is shown here:

.. code-block:: JSON


   {"@type": "RippleLogs", "timestamp": "2024-08-18T18:49:07Z", "level": "INFO", "msg": "127.0.0.1 - - [18/Aug/2024 18:49:07] \"\u001b[35m\u001b[1mPOST /processes/extract_submodel/execution HTTP/1.1\u001b[0m\" 201 -"}
   {"@type": "RippleLogs", "timestamp": "2024-08-18T18:49:13Z", "level": "INFO", "msg": "127.0.0.1 - - [18/Aug/2024 18:49:13] \"GET /jobs HTTP/1.1\" 200 -"}
   {"@type": "RippleLogs", "timestamp": "2024-08-18T18:51:40Z", "level": "INFO", "msg": "127.0.0.1 - - [18/Aug/2024 18:51:40] \"GET /jobs HTTP/1.1\" 200 -"}


Troubleshooting
----------------

Coming soon.