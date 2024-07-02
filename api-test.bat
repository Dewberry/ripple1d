@echo off

set API_TEST_SCRIPT="api\run_api_test.py"

call api-load-env.bat
if %ERRORLEVEL% neq 0 exit %ERRORLEVEL%

echo Loading virtual environment: "%VENV_PATH%"
call "%VENV_PATH%\Scripts\activate.bat"
if %ERRORLEVEL% neq 0 echo Press enter to exit. & set /p input= & exit /b %ERRORLEVEL%

echo Running script: %API_TEST_SCRIPT%
python -u "%API_TEST_SCRIPT%"
if %ERRORLEVEL% neq 0 (echo Error above with code %ERRORLEVEL%.) else (echo Success.)
echo Press enter to exit this test terminal. & set /p input=
