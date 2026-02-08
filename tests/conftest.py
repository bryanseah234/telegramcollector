import pytest
import asyncio
import sys
import os
from unittest.mock import MagicMock

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# MOCK BROKEN/HEAVY DEPENDENCIES
# We do this BEFORE importing application modules
sys.modules['av'] = MagicMock()
sys.modules['cv2'] = MagicMock()
sys.modules['insightface'] = MagicMock()
sys.modules['insightface.app'] = MagicMock()

# Set required environment variables for config.py
os.environ.setdefault('TG_API_ID', '12345')
os.environ.setdefault('TG_API_HASH', 'test_hash')
os.environ.setdefault('BOT_TOKEN', '123:test_token')
os.environ.setdefault('HUB_GROUP_ID', '-100123456789')
os.environ.setdefault('DB_PASSWORD', 'test_password')
os.environ.setdefault('REDIS_HOST', 'localhost')
os.environ.setdefault('REDIS_PORT', '6379')
# Avoid loading .env file which might confuse tests
os.environ['DOTENV_KEY'] = 'test' 

# Reload config to ensure it picks up these env vars if it was already imported
if 'config' in sys.modules:
    import importlib
    import config
    importlib.reload(config)

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
