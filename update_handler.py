"""
Update Handler - Monitors for update signals and triggers graceful restart.

Integrates with update_checker.sh via shared volume signal file.
"""
import os
import logging
import asyncio
import signal
import sys

logger = logging.getLogger(__name__)

SIGNAL_FILE = os.getenv('UPDATE_SIGNAL_FILE', '/app/signals/update_available')
CHECK_INTERVAL = 60  # Check every minute


class UpdateHandler:
    """
    Monitors for update signals and triggers application restart.
    """
    
    def __init__(self, shutdown_callback=None):
        """
        Args:
            shutdown_callback: Async function to call for graceful shutdown
        """
        self.shutdown_callback = shutdown_callback
        self._running = False
        self._task = None
    
    async def start(self):
        """Starts monitoring for update signals."""
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info(f"Update handler started. Monitoring: {SIGNAL_FILE}")
    
    async def stop(self):
        """Stops the update handler."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Update handler stopped")
    
    async def _monitor_loop(self):
        """Main monitoring loop."""
        while self._running:
            try:
                if self._check_update_signal():
                    await self._handle_update()
                
                await asyncio.sleep(CHECK_INTERVAL)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Update handler error: {e}")
                await asyncio.sleep(CHECK_INTERVAL)
    
    def _check_update_signal(self) -> bool:
        """Checks if update signal file exists."""
        return os.path.exists(SIGNAL_FILE)
    
    async def _handle_update(self):
        """Handles detected update."""
        logger.info("=" * 50)
        logger.info("UPDATE DETECTED - Initiating graceful shutdown...")
        logger.info("=" * 50)
        
        # Read new commit info
        try:
            with open(SIGNAL_FILE, 'r') as f:
                new_commit = f.read().strip()
            logger.info(f"New commit: {new_commit[:7] if new_commit else 'unknown'}")
        except Exception:
            pass
        
        # Remove signal file
        try:
            os.remove(SIGNAL_FILE)
            logger.info("Signal file cleared")
        except Exception as e:
            logger.warning(f"Could not remove signal file: {e}")
        
        # Trigger graceful shutdown
        if self.shutdown_callback:
            await self.shutdown_callback()
        
        # Exit process - Docker will restart us
        logger.info("Exiting for restart...")
        sys.exit(0)


def setup_update_handler(shutdown_callback=None) -> UpdateHandler:
    """
    Factory function to create and configure update handler.
    
    Usage in worker.py:
        from update_handler import setup_update_handler
        
        handler = setup_update_handler(worker.shutdown)
        await handler.start()
    """
    return UpdateHandler(shutdown_callback)
