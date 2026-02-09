#!/bin/bash
echo "========================================================"
echo "SYSTEM WIPE AND RESTART"
echo "WARNING: This will delete the DATABASE and REDIS data!"
echo "Session files in ./sessions will be PRESERVED."
echo "========================================================"
echo ""

echo "1. Stopping containers and removing volumes..."
docker-compose down -v

echo ""
echo "2. Rebuilding and starting the system..."
docker-compose up -d --build

echo ""
echo "========================================================"
echo "Done! System has been reset."
echo "========================================================"
