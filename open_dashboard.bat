@echo off
echo Detecting WSL IP Address...

:: Get the IP address of the WSL instance
for /f "tokens=*" %%i in ('wsl hostname -I') do set WSL_IP_RAW=%%i

:: Extract the first IP address (handling multiple IPs or trailing spaces)
for /f "tokens=1" %%a in ("%WSL_IP_RAW%") do set WSL_IP=%%a

if "%WSL_IP%"=="" (
    echo [ERROR] Could not detect WSL IP address.
    echo Please ensure WSL is running.
    pause
    exit /b 1
)

echo Found WSL IP: %WSL_IP%
echo Opening Dashboard at http://%WSL_IP%:8501 ...

:: Open default browser
start http://%WSL_IP%:8501

:: detailed logging info
echo.
echo If the page does not load:
echo 1. Ensure the docker containers are running: 'docker-compose ps'
echo 2. Check logs: 'docker-compose logs dashboard'
echo.
timeout /t 5
