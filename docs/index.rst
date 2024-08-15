.. ripple1d documentation master file, created by
   sphinx-quickstart on Sun Aug  4 09:14:49 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Ripple
======

:code:`ripple1d` is a Python utility for repurposing HEC-RAS models for use in the production 
of Flood Inundation Maps (FIMs) and rating curves for use in near-real-time flood forecasting 
on the NOAA National Water Model network.

.. note::
   This version is tagged as **experimental**. Features and APIs are subject to change.

Usage
~~~~~

When successfully installed, a standalone executable will be available (stored in the path) allowing you to manage the 
Flask API and Huey consumer direcly by calling ``ripple1d`` in either a Command Prompt or PowerShell terminal
Below are the steps to start, stop, and check the status of the Ripple Manager..

**Start the Ripple Services**:

   .. code-block:: powershell

      ripple1d start  --flask_port 5000 --thread_count 5 


**Help for the Ripple Services**:

   .. code-block:: powershell

      ripple1d -h

      ripple1d start -h

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   ripple1d/index
   changes.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`