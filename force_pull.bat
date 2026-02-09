@echo off
echo ========================================================
echo FORCE PULL SCRIPT
echo WARNING: This will discard ALL local changes and commits!
echo ========================================================
echo.
echo 1. Stopping Docker containers to release file locks...
docker-compose down

echo.
echo 2. Fetching latest changes from origin...
git fetch origin

echo.
echo 2. Resetting local branch to match origin/main...
git reset --hard origin/main

echo.
echo ========================================================
echo Done! Local codebase is now exactly synced with origin/main.
echo ========================================================
pause
