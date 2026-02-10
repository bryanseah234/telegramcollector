
import asyncio
import logging
from unittest.mock import MagicMock
from enum import Enum

# Mock classes for standalone testing
class BackpressureState(Enum):
    NORMAL = 'normal'
    WARNING = 'warning'
    CRITICAL = 'critical'

class MockProcessingQueue:
    def __init__(self):
        self._backpressure_state = BackpressureState.CRITICAL
        self.queue_size = 150 # Start high
        self.check_calls = 0
        
    def should_pause(self):
        return self._backpressure_state == BackpressureState.CRITICAL

    def check_backpressure(self):
        self.check_calls += 1
        print(f"DEBUG: check_backpressure called (Call #{self.check_calls}, Queue Size: {self.queue_size})")
        # Simulate draining
        self.queue_size -= 50
        if self.queue_size < 100:
            self._backpressure_state = BackpressureState.NORMAL
        
class MockScanner:
    def __init__(self, queue):
        self.processing_queue = queue
        
    async def scan_loop(self):
        print("Starting scan loop...")
        
        # Simulate finding a message
        print("Found message 1")
        
        # Original Buggy Logic (If we didn't have the fix)
        # while self.processing_queue.should_pause():
        #     print("Paused...")
        #     await asyncio.sleep(1)
            
        # Fixed Logic
        print("Checking backpressure...")
        while self.processing_queue.should_pause():
            print("Combined Queue limit reached. Pausing scan...")
            
            # THE FIX:
            self.processing_queue.check_backpressure()
            
            if not self.processing_queue.should_pause():
                print("Combined Queue limit dropped below critical. Resuming scan...")
                break
                
            await asyncio.sleep(0.1) # Fast sleep for test
            
        print("Resumed! Scan continues.")
        print("Test PASSED")

async def main():
    logging.basicConfig(level=logging.DEBUG)
    queue = MockProcessingQueue()
    scanner = MockScanner(queue)
    
    # Run with timeout to detect infinite loop
    try:
        await asyncio.wait_for(scanner.scan_loop(), timeout=5.0)
    except asyncio.TimeoutError:
        print("Test FAILED: Scanner stuck in infinite loop!")

if __name__ == "__main__":
    asyncio.run(main())
