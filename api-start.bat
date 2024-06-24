@echo off

:: Identify the full path to huey_consumer.py (ships with huey library)
set huey_consumer_full_path=
for /f "delims=" %%i in ('where huey_consumer.py') do set huey_consumer_full_path=%%i

:: Launch huey consumer in separate terminal
set logs_dir="api\logs\"
echo "Deleting logs dir if exists: %logs_dir%"
rmdir /s /q "api\logs\"
echo "Creating logs dir: %logs_dir%"
if not exist "api\logs\" mkdir "api\logs\"
echo "Starting ripple-huey"
start "ripple-huey" cmd /k "python -u %huey_consumer_full_path% api.tasks.huey -w 1 -l api\logs\huey-consumer.log"

:: Launch Flask app in a separate terminal
echo "Starting ripple-flask"
start "ripple-flask" cmd /k "flask run"
