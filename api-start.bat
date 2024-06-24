@echo off

:: Identify the full path to huey_consumer.py (ships with huey library)
set huey_consumer_full_path=
for /f "delims=" %%i in ('where huey_consumer.py') do set huey_consumer_full_path=%%i

:: Set up logs dir
set logs_dir="api\logs\foo\bar"
echo "Deleting logs dir if exists: %logs_dir%"
rmdir /s /q %logs_dir%
if exist %logs_dir% (
    echo Error: could not delete %logs_dir%
    echo Press any key to exit.
    set /p input=
    exit /b 1
)
echo "Creating logs dir: %logs_dir%"
if not exist %logs_dir% mkdir %logs_dir%
if %errorlevel% neq 0 (
    echo Error: could not create %logs_dir%
    echo Press any key to exit.
    set /p input=
    exit /b 1
)

:: Launch huey consumer in separate terminal
echo "Starting ripple-huey"
start "ripple-huey" cmd /k "python -u %huey_consumer_full_path% api.tasks.huey -w 1 -l api\logs\huey-consumer.log"

:: Launch Flask app in a separate terminal
echo "Starting ripple-flask"
start "ripple-flask" cmd /k "flask run"
