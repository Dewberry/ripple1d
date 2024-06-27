@echo off

set api_test_script="api\run_api_test.py"
echo Running script: %api_test_script%

python -u "%api_test_script%"

if %errorlevel% neq 0 (echo Error above with code %errorlevel%.) else (echo Success.)
echo Press enter to exit this test terminal.
set /p input=
