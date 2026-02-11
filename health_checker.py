"""
Health Checker - Periodic system health monitoring.

Runs health checks and stores results for dashboard display.
Optional feature that can be enabled/disabled via environment.
"""
import logging
import asyncio
import os
import time
from typing import Dict, Any
from database import get_db_connection

logger = logging.getLogger(__name__)


class HealthChecker:
    """
    Performs periodic health checks on all system components.
    Stores results in database for dashboard display.
    """
    
    def __init__(
        self,
        client=None,
        face_processor=None,
        processing_queue=None,
        check_interval: int = 1800  # 30 minutes default
    ):
        """
        Args:
            client: Telegram client instance
            face_processor: FaceProcessor instance
            processing_queue: ProcessingQueue instance
            check_interval: Seconds between health checks
        """
        self.client = client
        self.face_processor = face_processor
        self.processing_queue = processing_queue
        self.check_interval = check_interval
        
        self._running = False
        self._task = None
        self._recovery_actions = {} # check_name -> async callback
    
    async def start(self):
        """Starts periodic health checks."""
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"Health checker started (interval: {self.check_interval}s)")
    
    async def stop(self):
        """Stops health checks."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Health checker stopped")
    
    async def _run_loop(self):
        """Main loop that runs periodic checks."""
        while self._running:
            try:
                await self.run_checks()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                await asyncio.sleep(60)  # Wait before retrying
    
    def register_recovery(self, check_name: str, callback):
        """Registers a callback to be executed if a check fails."""
        self._recovery_actions[check_name] = callback
        logger.info(f"Registered recovery action for: {check_name}")
    
    async def run_checks(self) -> Dict[str, Any]:
        """
        Runs all health checks and stores results.
        
        Returns:
            Dict with check results
        """
        start_time = time.time()
        results = {
            'database_ok': False,
            'telegram_ok': False,
            'face_model_ok': False,
            'hub_access_ok': False,
            'queue_size': 0,
            'workers_active': 0,
            'error_message': None
        }
        
        errors = []
        
        # 1. Database check
        try:
            results['database_ok'] = await self._check_database()
        except Exception as e:
            errors.append(f"Database: {e}")
        
        # 2. Telegram check
        try:
            results['telegram_ok'] = await self._check_telegram()
        except Exception as e:
            errors.append(f"Telegram: {e}")
        
        # 3. Face model check
        try:
            results['face_model_ok'] = self._check_face_model()
        except Exception as e:
            errors.append(f"Face model: {e}")
        
        # 4. Hub access check
        try:
            results['hub_access_ok'] = await self._check_hub_access()
        except Exception as e:
            errors.append(f"Hub access: {e}")
        
        # 5. Queue status
        if self.processing_queue:
            results['queue_size'] = self.processing_queue.get_queue_size()
            results['workers_active'] = len(self.processing_queue._workers)
        
        # Combine errors
        if errors:
            results['error_message'] = "; ".join(errors)
        
        # Calculate response time
        response_time_ms = int((time.time() - start_time) * 1000)
        
        # 6. Execute Recovery Actions
        if errors:
            await self._run_recovery(results)

        # Store results
        await self._store_results(results, response_time_ms)
        
        # Log summary
        status = "✓" if all([
            results['database_ok'],
            results['telegram_ok'],
            results['face_model_ok']
        ]) else "✗"
        logger.info(f"Health check {status} (response: {response_time_ms}ms, queue: {results['queue_size']})")
        
        return results

    async def _run_recovery(self, results: Dict[str, Any]):
        """Attempts to run recovery actions for failed checks."""
        for check_name, ok in results.items():
            if check_name.endswith('_ok') and not ok:
                name = check_name.replace('_ok', '')
                if name in self._recovery_actions:
                    logger.warning(f"Attempting recovery for {name}...")
                    try:
                        await self._recovery_actions[name]()
                        logger.info(f"Recovery attempt for {name} completed")
                    except Exception as e:
                        logger.error(f"Recovery action for {name} failed: {e}")
    
    async def _check_database(self) -> bool:
        """Tests database connectivity."""
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
                    result = await cursor.fetchone()
                    return result is not None
        except Exception:
            return False
    
    async def _check_telegram(self) -> bool:
        """Tests Telegram connection and authorization."""
        if not self.client:
            return False
        
        if not self.client.is_connected():
            return False
        
        return await self.client.is_user_authorized()
    
    def _check_face_model(self) -> bool:
        """Tests if face model is loaded."""
        if not self.face_processor:
            return False
        
        return hasattr(self.face_processor, 'app') and self.face_processor.app is not None
    
    async def _check_hub_access(self) -> bool:
        """Tests if we can access the hub group."""
        if not self.client:
            return False
        
        hub_group_id = int(os.getenv('HUB_GROUP_ID', 0))
        if not hub_group_id:
            return False
        
        try:
            entity = await self.client.get_entity(hub_group_id)
            return entity is not None
        except Exception:
            return False
    
    async def _store_results(self, results: Dict[str, Any], response_time_ms: int):
        """Stores health check results in database."""
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO health_checks 
                            (database_ok, telegram_ok, face_model_ok, hub_access_ok,
                             queue_size, workers_active, error_message, response_time_ms)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        results['database_ok'],
                        results['telegram_ok'],
                        results['face_model_ok'],
                        results['hub_access_ok'],
                        results['queue_size'],
                        results['workers_active'],
                        results['error_message'],
                        response_time_ms
                    ))
                    # autocommit should be on, but if not:
                    # await conn.commit() 
        except Exception as e:
            logger.error(f"Failed to store health check: {e}")
        except Exception as e:
            logger.error(f"Failed to store health check: {e}")
    
    async def get_latest_status(self) -> Dict[str, Any]:
        """Retrieves the most recent health check from database."""
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT check_time, database_ok, telegram_ok, face_model_ok, 
                               hub_access_ok, queue_size, workers_active, error_message
                        FROM health_checks
                        ORDER BY check_time DESC
                        LIMIT 1
                    """)
                    row = await cursor.fetchone()
                    
                    if row:
                        return {
                            'check_time': row[0],
                            'database_ok': row[1],
                            'telegram_ok': row[2],
                            'face_model_ok': row[3],
                            'hub_access_ok': row[4],
                            'queue_size': row[5],
                            'workers_active': row[6],
                            'error_message': row[7],
                            'all_ok': all([row[1], row[2], row[3], row[4]])
                        }
                    return None
        except Exception as e:
            logger.error(f"Failed to get health status: {e}")
            return None
