"""
Bot Commands - Handles interactive commands from the Hub Group.

Commands:
/status - System health check
/pause - Pause message scanning
/resume - Resume message scanning
/restart - Restart the application
"""
import logging
import asyncio
import sys
import os
from telethon import events
from config import settings

logger = logging.getLogger(__name__)

async def handle_status(event, worker):
    """Handles /status command."""
    try:
        if not await _is_admin(event, worker):
            return

        status_msg = await event.reply("🔍 Checking system status...")
        
        # Trigger health check in worker
        await worker._run_health_checks()
        
        # Delete the "Checking..." message since the report is a new message
        await status_msg.delete()
        
    except Exception as e:
        logger.error(f"Status command failed: {e}")
        await event.reply(f"❌ Error: {str(e)}")

async def handle_pause(event, worker):
    """Handles /pause command."""
    try:
        if not await _is_admin(event, worker):
            return
            
        if not worker.processing_queue:
            await event.reply("❌ Processing queue not initialized.")
            return

        worker.processing_queue.set_manual_pause(True)
        
        # Persist to Redis
        if worker.processing_queue.redis_available:
             worker.processing_queue.redis_client.set("system_paused", "true")

        await event.reply("II **System Paused**\nScanning has been suspended. Queue processing continues until empty.")
        
    except Exception as e:
        logger.error(f"Pause command failed: {e}")
        await event.reply(f"❌ Error: {str(e)}")

async def handle_resume(event, worker):
    """Handles /resume command."""
    try:
        if not await _is_admin(event, worker):
            return

        if not worker.processing_queue:
            await event.reply("❌ Processing queue not initialized.")
            return

        worker.processing_queue.set_manual_pause(False)
        
        # Persist to Redis
        if worker.processing_queue.redis_available:
             worker.processing_queue.redis_client.delete("system_paused")

        await event.reply("▶️ **System Resumed**\nScanning and processing active.")
        
    except Exception as e:
        logger.error(f"Resume command failed: {e}")
        await event.reply(f"❌ Error: {str(e)}")

async def handle_restart(event, worker):
    """Handles /restart command."""
    try:
        if not await _is_admin(event, worker):
            return

        await event.reply("🔄 **Restarting System...**\nProcess will exit and Docker should auto-restart it.")
        logger.warning("Restart requested via Bot Command.")
        
        # Wait a moment for message to send
        await asyncio.sleep(1)
        
        # Trigger explicit shutdown
        await worker.shutdown()
        
        # Force exit
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Restart command failed: {e}")
        await event.reply(f"❌ Error: {str(e)}")

async def handle_help(event, worker):
    """Handles /help or /commands command."""
    try:
        if not await _is_admin(event, worker):
            return

        help_text = (
            "🤖 **Telegram Archiver Bot - Commands**\n\n"
            "🔍 `/status` - Check system health and queue depth.\n"
            "⏸️ `/pause` - Suspend message scanning (queue continues).\n"
            "▶️ `/resume` - Resume scanning and processing.\n"
            "🔄 `/restart` - Safely restart the application via Docker.\n"
            "❓ `/commands` - Show this help menu."
        )
        await event.reply(help_text)
        
    except Exception as e:
        logger.error(f"Help command failed: {e}")
        await event.reply(f"❌ Error: {str(e)}")

async def _is_admin(event, worker) -> bool:
    """Verifies if the sender is an admin in the Hub Group."""
    try:
        sender_id = event.sender_id
        chat_id = event.chat_id
        
        # Check if in Hub Group
        if chat_id != settings.HUB_GROUP_ID:
            return False
            
        # Check if Anonymous Admin (sender is the group itself)
        if sender_id == chat_id:
            return True
            
        # Check if user is admin via Telethon permissions
        # We need the bot client from the event
        client = event.client
        perms = await client.get_permissions(chat_id, sender_id)
        
        if perms.is_admin or perms.is_creator:
            return True
            
        return False
        
    except Exception as e:
        logger.error(f"Permission check failed: {e}")
        return False
