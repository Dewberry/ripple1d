@echo off
:: This sets environment variables based on file ".env".
:: This is called by api-start.bat and by api-test.bat.

:: Load KEY=VALUE pairs from .env file (https://stackoverflow.com/questions/232747/read-environment-variables-from-file-in-windows-batch-cmd-exe)

if not exist .env (
    echo Error: could not find path: .env
    echo Press enter to exit. & set /p input= & exit /b 1
)
for /F "delims== tokens=1,* eol=#" %%i in (.env) do set %%i=%%~j

:: Check variable VENV_PATH
if not defined VENV_PATH (
    echo Error: variable VENV_PATH not defined.
    echo Press enter to exit. & set /p input= & exit /b 1
)
:: If VENV_PATH contains variables, such as a path shortcut like %HOMEPATH%, expand those (https://superuser.com/questions/1750109/how-to-expand-a-variable-containing-another-variable-in-a-windows-command-prompt)
for /f "tokens=*" %%a in ('echo %VENV_PATH%') do @set VENV_PATH=%%a
if not exist "%VENV_PATH%" (
    echo Error: could not find virtual environment path: "%VENV_PATH%"
    echo Press enter to exit. & set /p input= & exit /b 1
)

:: Check variable HUEY_THREAD_COUNT
if not defined HUEY_THREAD_COUNT (
    echo Error: variable HUEY_THREAD_COUNT not defined.
    echo Press enter to exit. & set /p input= & exit /b 1
)
