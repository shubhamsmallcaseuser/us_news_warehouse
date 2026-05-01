@echo off
setlocal

REM set configs
set PROJECT_DIR="C:\smallcase\us_news\us_news_warehouse"
set PYTHON_EXE="C:\Users\wm-eikon-user\anaconda3\envs\env_srivatsa\python.exe"
set SCRIPT="us_news_warehousing_v1.py"

REM change to project directory
cd /d "%PROJECT_DIR%" || exit /b %ERRORLEVEL%

REM run the script
"%PYTHON_EXE%" "%SCRIPT%"