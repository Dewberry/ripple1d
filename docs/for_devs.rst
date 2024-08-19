
:code:`ripple1d` is a Python utility for repurposing HEC-RAS models for use in the production 
of Flood Inundation Maps (FIMs) and rating curves for use in near-real-time flood forecasting 
on the NOAA National Water Model network.

Quick start
-----------

Ripple1d is registered with `PyPI <https://pypi.org/project/ripple1d>`_ and can be installed simply using python's pip package installer. 
Assuming you have Python already installed and setup:

.. prompt:: powershell $
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


    **Server log**: C:\Users\user\Desktop\2024-08-18T18-46-42.828592+00-00-ripple1d-flask.jsonld
     
    **Jobs log**: C:\Users\user\Desktop\2024-08-18T18-46-42.376085+00-00-ripple1d-huey.jsonld

