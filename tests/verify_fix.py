import asyncio
import logging
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("verification")

# Load environment
load_dotenv()

async def verify_hub_accessibility():
    """
    Simulates the startup and priming flow to verify Hub accessibility.
    """
    from bot_pool import bot_pool
    from bot_client import bot_client_manager
    from hub_notifier import HubNotifier
    from config import settings
    
    logger.info("Starting verification script...")
    
    # 1. Initialize Bot Pool (this triggers priming)
    try:
        await bot_client_manager.start()
        logger.info("✓ BotClientManager started and bots primed")
    except Exception as e:
        logger.error(f"✗ Failed to start BotClientManager: {e}")
        return

    # 2. Get a client and check if entity is cached
    client = bot_client_manager.client
    hub_id = settings.HUB_GROUP_ID
    
    logger.info(f"Checking Hub ID: {hub_id}")
    
    try:
        # get_input_entity should work now because it was primed (cached)
        entity = await client.get_input_entity(hub_id)
        logger.info(f"✓ Successfully got input entity for Hub: {entity}")
    except Exception as e:
        logger.error(f"✗ Failed to get input entity for Hub: {e}")
        logger.info("Attempting full resolution (fallback check)...")
        try:
            entity = await client.get_entity(hub_id)
            logger.info(f"✓ Full resolution worked: {entity}")
        except Exception as e2:
            logger.error(f"✗ Full resolution also failed: {e2}")

    # 3. Test Hub Notification
    notifier = HubNotifier.get_instance()
    # We don't start the background flusher for this quick test, just call _send_message directly
    logger.info("Testing immediate notification...")
    success = await notifier._send_message("🛠️ **Verification Test**: System accessibility fix applied.")
    if success:
        logger.info("✓ Hub notification sent successfully!")
    else:
        logger.error("✗ Hub notification failed.")

    await bot_client_manager.disconnect()
    logger.info("Verification complete.")

if __name__ == "__main__":
    asyncio.run(verify_hub_accessibility())
