@echo off
setlocal

REM set configs
set PROJECT_DIR="C:\smallcase\srivatsa_rao\github_repos\wm-factor-scores\wm_utils\misc_jobs"
set PYTHON_EXE="C:\Users\wm-eikon-user\anaconda3\envs\env_srivatsa\python.exe"
set SCRIPT="announcements.py"

REM change to project directory
cd /d "%PROJECT_DIR%" || exit /b %ERRORLEVEL%

REM run the script
"%PYTHON_EXE%" "%SCRIPT%"
