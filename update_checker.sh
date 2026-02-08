#!/bin/sh
# Update Checker - Monitors GitHub for new commits and signals app to restart
# Runs in a lightweight Alpine container

SIGNAL_FILE="/app/signals/update_available"
LAST_COMMIT_FILE="/app/signals/last_commit"

# Ensure signals directory exists
mkdir -p /app/signals

# Get initial commit hash
get_latest_commit() {
    if [ -z "$GITHUB_REPO" ]; then
        echo ""
        return
    fi
    
    curl -s "https://api.github.com/repos/${GITHUB_REPO}/commits/${GITHUB_BRANCH}" | \
        grep '"sha"' | head -1 | cut -d'"' -f4
}

# Initialize last known commit
LAST_COMMIT=$(cat "$LAST_COMMIT_FILE" 2>/dev/null || echo "")

if [ -z "$LAST_COMMIT" ]; then
    LAST_COMMIT=$(get_latest_commit)
    echo "$LAST_COMMIT" > "$LAST_COMMIT_FILE"
    echo "[Update Checker] Initialized with commit: ${LAST_COMMIT:0:7}"
fi

echo "[Update Checker] Starting. Checking every ${CHECK_INTERVAL:-1800}s"
echo "[Update Checker] Repository: ${GITHUB_REPO:-not set}"
echo "[Update Checker] Branch: ${GITHUB_BRANCH:-main}"

# Main loop
while true; do
    sleep "${CHECK_INTERVAL:-1800}"
    
    if [ -z "$GITHUB_REPO" ]; then
        echo "[Update Checker] No repository configured. Skipping check."
        continue
    fi
    
    CURRENT_COMMIT=$(get_latest_commit)
    
    if [ -z "$CURRENT_COMMIT" ]; then
        echo "[Update Checker] Failed to fetch commit. Will retry."
        continue
    fi
    
    if [ "$CURRENT_COMMIT" != "$LAST_COMMIT" ]; then
        echo "[Update Checker] Update detected!"
        echo "[Update Checker] Old: ${LAST_COMMIT:0:7} -> New: ${CURRENT_COMMIT:0:7}"
        
        # Create signal file
        echo "$CURRENT_COMMIT" > "$SIGNAL_FILE"
        echo "$CURRENT_COMMIT" > "$LAST_COMMIT_FILE"
        LAST_COMMIT="$CURRENT_COMMIT"
        
        echo "[Update Checker] Signal file created. Waiting for app to restart..."
    else
        echo "[Update Checker] No updates. Commit: ${CURRENT_COMMIT:0:7}"
    fi
done
