@echo off

call api-load-env.bat
if %ERRORLEVEL% neq 0 exit %ERRORLEVEL%

:: Identify the full path to huey_consumer.py (ships with huey library)
set HUEY_CONSUMER_FULL_PATH=
for /f "delims=" %%i in ('where huey_consumer.py') do set HUEY_CONSUMER_FULL_PATH=%%i
if not exist "%HUEY_CONSUMER_FULL_PATH%" (
    echo Error: huey consumer script was not discoverable.
    echo Press enter to exit. & set /p input= & exit /b 1
)

:: Set up logs dir
set LOGS_DIR="api\logs\"
echo "Deleting logs dir if exists: %LOGS_DIR%"
rmdir /s /q %LOGS_DIR%
if exist %LOGS_DIR% (
    echo Error: could not delete %LOGS_DIR%
    echo Press enter to exit. & set /p input= & exit /b 1
)
echo "Creating logs dir: %LOGS_DIR%"
if not exist %LOGS_DIR% mkdir %LOGS_DIR%
if %ERRORLEVEL% neq 0 (
    echo Error: could not create %LOGS_DIR%
    echo Press enter to exit. & set /p input= & exit /b 1
)

:: Launch huey consumer in separate terminal
echo "Starting ripple-huey"
start "ripple-huey" cmd /k ""%VENV_PATH%\Scripts\activate.bat" && python -u %HUEY_CONSUMER_FULL_PATH% api.tasks.huey -w %HUEY_THREAD_COUNT%"

:: Launch Flask app in a separate terminal
echo "Starting ripple-flask"
start "ripple-flask" cmd /k ""%VENV_PATH%\Scripts\activate.bat" && flask run"
