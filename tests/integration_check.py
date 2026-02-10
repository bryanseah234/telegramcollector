
import asyncio
import sys
import os
import logging
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

async def test_integrations():
    print("🚀 Starting Integration Check...", flush=True)

    # 1. Test Config Loading
    print("🔹 Testing Config...", flush=True)
    try:
        # Patch redis in config to avoid connection attempt
        with patch('config.redis', None):
            from config import settings
            print(f"   ✅ Config loaded. QUEUE_MAX_SIZE={settings.QUEUE_MAX_SIZE}, WORKERS={settings.NUM_WORKERS}, GPU={settings.USE_GPU}", flush=True)
    except Exception as e:
        print(f"   ❌ Config loading failed: {e}", flush=True)
        return

    # 2. Test FaceProcessor Initialization (with Failsafe)
    print("🔹 Testing FaceProcessor (init logic)...")
    try:
        from face_processor import FaceProcessor
        # Mock insightface to simulate GPU failure behavior
        with patch.dict('sys.modules', {'insightface': MagicMock(), 'insightface.app': MagicMock()}):
            fp = FaceProcessor.get_instance()
            # Simulate lazy init
            with patch('face_processor.FaceAnalysis') as MockFaceAnalysis:
                # Scenario: First init fails (simulating GPU error), second init works (simulating CPU fallback)
                MockFaceAnalysis.side_effect = [Exception("CUDA detection failed"), MagicMock()]
                
                success = fp._lazy_init()
                if success:
                    print("   ✅ FaceProcessor initialized (Failsafe triggered successfully)")
                else:
                    print("   ❌ FaceProcessor failsafe check failed")
    except Exception as e:
        print(f"   ❌ FaceProcessor logic failed: {e}")

    # 3. Test ProcessingQueue Logic
    print("🔹 Testing ProcessingQueue...")
    try:
        from processing_queue import ProcessingQueue, BackpressureState
        # Mock Redis
        with patch('processing_queue.redis.Redis'):
            pq = ProcessingQueue()
            
            # Test backpressure logic we modified
            pq._redis = MagicMock()
            pq._redis.llen.return_value = 5000  # Simulate FULL queue
            
            pq.check_backpressure()  # Should trigger CRITICAL
            
            if pq.should_pause():
                print("   ✅ Backpressure logic working (Paused on high queue)")
            else:
                print("   ❌ Backpressure logic FAILED (Did not pause)")
                
            # Simulate DRAIN
            pq._redis.llen.return_value = 100
            pq.check_backpressure()
            
            if not pq.should_pause():
                print("   ✅ Backpressure release working (Resumed on low queue)")
            else:
                print("   ❌ Backpressure release FAILED (Stuck in pause)")
                
    except Exception as e:
         print(f"   ❌ ProcessingQueue check failed: {e}")

    # 4. Test Scanner Integration
    print("🔹 Testing MessageScanner Integration...")
    try:
        from message_scanner import MessageScanner
        from processing_queue import ProcessingQueue
        
        mock_client = MagicMock()
        mock_queue = MagicMock(spec=ProcessingQueue)
        mock_queue.should_pause.return_value = False # Initially okay
        
        scanner = MessageScanner(client=mock_client, processing_queue=mock_queue)
        print("   ✅ MessageScanner instantiated successfully")
        
    except Exception as e:
        print(f"   ❌ MessageScanner instantiation failed: {e}")

    print("🎉 Integration Check Complete.")

if __name__ == "__main__":
    try:
        asyncio.run(test_integrations())
    except KeyboardInterrupt:
        pass
