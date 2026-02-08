"""
Update Checker - Monitors GitHub for updates and triggers container restarts.

Checks the configured GitHub repository for new commits and
signals for container update when changes are detected.
"""
import logging
import asyncio
import os
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

class UpdateChecker:
    """
    Monitors a GitHub repository for updates.
    """
    
    def __init__(self):
        self.repo = os.getenv('GITHUB_REPO', '')
        self.branch = os.getenv('GITHUB_BRANCH', 'main')
        self.check_interval = int(os.getenv('UPDATE_CHECK_INTERVAL', 1800))  # 30 minutes
        self.last_commit_sha = None
        
        if not self.repo:
            logger.warning("GITHUB_REPO not set. Update checker disabled.")
    
    async def start_monitoring(self):
        """
        Starts the update monitoring loop.
        """
        if not self.repo:
            return
        
        logger.info(f"Starting update checker for {self.repo}@{self.branch}")
        
        while True:
            try:
                await self._check_for_updates()
            except Exception as e:
                logger.error(f"Update check failed: {e}")
            
            await asyncio.sleep(self.check_interval)
    
    async def _check_for_updates(self):
        """
        Checks GitHub API for new commits.
        """
        url = f"https://api.github.com/repos/{self.repo}/commits/{self.branch}"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            current_sha = data['sha']
            
            if self.last_commit_sha is None:
                self.last_commit_sha = current_sha
                logger.info(f"Initial commit SHA: {current_sha[:8]}")
            elif current_sha != self.last_commit_sha:
                logger.info(f"New commit detected: {current_sha[:8]}")
                self.last_commit_sha = current_sha
                await self._trigger_update()
            else:
                logger.debug("No new commits found.")
                
        except requests.RequestException as e:
            logger.warning(f"Failed to check GitHub: {e}")
    
    async def _trigger_update(self):
        """
        Triggers the update process (e.g., write flag file for Watchtower).
        """
        # TODO: Implement update trigger mechanism
        # Options:
        # 1. Write a flag file that a watcher script monitors
        # 2. Send a signal to the main process
        # 3. Exit with a specific code for Docker to restart
        
        logger.info("Update available. Signaling for restart...")
        
        # Example: Write update flag file
        with open('/app/data/.update_available', 'w') as f:
            f.write(datetime.now().isoformat())
