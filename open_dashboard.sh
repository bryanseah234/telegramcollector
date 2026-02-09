#!/bin/bash
echo "Detecting WSL IP Address..."

# Get the first IP address of the WSL instance
WSL_IP=$(hostname -I | awk '{print $1}')

if [ -z "$WSL_IP" ]; then
    echo "[ERROR] Could not detect WSL IP address."
    exit 1
fi

echo "Found WSL IP: $WSL_IP"
echo "Opening Dashboard at http://$WSL_IP:8501 ..."

# Open default Windows browser using cmd.exe
# We use cmd.exe /C start to launch the Windows default browser handler
cmd.exe /C start "http://$WSL_IP:8501"

echo "Command sent to Windows."
