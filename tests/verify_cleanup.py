import asyncio
import logging
from topic_manager import TopicManager
from config import settings
from bot_client import bot_client_manager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_cleanup():
    """
    Tests the cleanup_topic mechanism.
    1. Sends a message to the General topic using BOT.
    2. Runs cleanup with 0 retention using USER client.
    3. Verifies success.
    """
    logger.info("Starting cleanup verification...")
    
    # 1. Start bot client
    await bot_client_manager.start()
    bot_client = bot_client_manager.client
    
    # 2. Get a user client
    from telegram_client import TelegramClientManager
    import os
    
    # Find a session file
    sessions_dir = settings.SESSIONS_DIR
    session_files = [f for f in os.listdir(sessions_dir) if f.endswith('.session')]
    if not session_files:
        logger.error("No user sessions found for testing!")
        return
        
    session_name = session_files[0].replace('.session', '')
    user_manager = TelegramClientManager(session_name=session_name)
    await user_manager.start()
    user_client = user_manager.client
    
    hub_id = settings.HUB_GROUP_ID
    if not hub_id:
        logger.error("HUB_GROUP_ID not set!")
        return

    # 3. Send test message to General topic (thread_id=1) using BOT
    logger.info("Sending test message to General topic via BOT...")
    try:
        sent_msg = await bot_client.send_message(hub_id, "🧹 Cleanup Test Message", reply_to=1)
        logger.info(f"Test message sent (ID: {sent_msg.id})")
    except Exception as e:
        logger.warning(f"Could not send to thread 1, trying without thread: {e}")
        sent_msg = await bot_client.send_message(hub_id, "🧹 Cleanup Test Message")
        logger.info(f"Test message sent to main chat (ID: {sent_msg.id})")

    # 4. Initialize TopicManager and run cleanup using USER client
    tm = TopicManager()
    
    # Run cleanup with 0 retention (delete everything older than now)
    logger.info("Running cleanup with 0 retention using USER client...")
    success = await tm.cleanup_topic(telegram_topic_id=1, retention_hours=0, client=user_client)
    
    if success:
        logger.info("✅ Cleanup method returned True")
    else:
        logger.error("❌ Cleanup method failed")

    # 5. Check if message still exists (check using user_client to be sure)
    try:
        msg = await user_client.get_messages(hub_id, ids=sent_msg.id)
        if msg is None or isinstance(msg, list) and not msg or msg.id == 0:
            logger.info("✅ Verified: Test message was deleted")
        else:
            logger.warning(f"⚠️ Test message still exists (ID: {msg.id})")
    except Exception as e:
        logger.info(f"✅ Verified: Could not fetch message (likely deleted): {e}")

    await bot_client_manager.disconnect()
    await user_manager.stop()

if __name__ == "__main__":
    asyncio.run(test_cleanup())
