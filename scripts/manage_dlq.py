
import asyncio
import argparse
import json
import os
import sys
import base64
import io
import logging
from datetime import datetime
import redis
from telethon import TelegramClient

# Add parent directory to path to import config/database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from database import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DLQ_KEY = "processing_queue:dead_letter"
MAIN_QUEUE_KEY = "processing_queue:tasks"

def get_redis_client():
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD,
        decode_responses=False # We handle bytes manually
    )

def inspect_dlq(limit=10):
    r = get_redis_client()
    count = r.llen(DLQ_KEY)
    print(f"\n📋 Dead Letter Queue Status: {count} failed tasks\n")
    
    if count == 0:
        return

    items = r.lrange(DLQ_KEY, 0, limit - 1)
    print(f"Showing last {len(items)} items:")
    print("-" * 60)
    
    for i, item in enumerate(items):
        try:
            data = json.loads(item)
            reason = data.get('_failure_reason', 'Unknown')
            date = data.get('_failed_at', 'Unknown time')
            chat = data.get('chat_id', 'Unknown')
            msg = data.get('message_id', 'Unknown')
            media = data.get('media_type', 'unknown')
            
            print(f"[{i+1}] {date}")
            print(f"    Reason: {reason}")
            print(f"    Target: Chat {chat}, Msg {msg} ({media})")
            print("-" * 60)
        except Exception as e:
            print(f"[{i+1}] Error parsing item: {e}")

def clear_dlq():
    r = get_redis_client()
    count = r.llen(DLQ_KEY)
    if count == 0:
        print("DLQ is already empty.")
        return
    
    confirm = input(f"⚠️  Are you sure you want to delete {count} items? (y/n): ")
    if confirm.lower() == 'y':
        r.delete(DLQ_KEY)
        print("✅ DLQ cleared.")
    else:
        print("Cancelled.")

async def get_account_session(chat_id):
    """Finds which account is monitoring this chat."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT a.session_file_path, a.phone_number 
                FROM scan_checkpoints sc
                JOIN telegram_accounts a ON sc.account_id = a.id
                WHERE sc.chat_id = %s
            """, (chat_id,))
            row = await cur.fetchone()
            return row if row else None

async def retry_tasks(limit=0):
    r = get_redis_client()
    count = r.llen(DLQ_KEY)
    
    if count == 0:
        print("No tasks to retry.")
        return
        
    to_process = count if limit == 0 else min(limit, count)
    print(f"🔄 Attempting to retry {to_process} tasks...")
    
    # Cache clients: session_path -> TelegramClient
    clients = {}
    
    success_count = 0
    fail_count = 0
    
    # Process oldest first (Right Pop from DLQ? No, standard is LPOP usually for queue, 
    # but DLQ piles up. Let's just iterate and process.)
    # Note: Accessing by index or popping? 
    # Safer to POP one by one, process, and if fail push back? 
    # Or just read all then process.
    # Let's simple: LPOP -> Process -> RPUSH to Main if success, RPUSH back to DLQ if fail.
    
    for _ in range(to_process):
        # Pop from left (FIFO)
        item_bytes = r.lpop(DLQ_KEY)
        if not item_bytes:
            break
            
        try:
            data = json.loads(item_bytes)
            chat_id = data.get('chat_id')
            message_id = data.get('message_id')
            
            if not chat_id or not message_id:
                print(f"Skipping invalid item: {data.keys()}")
                fail_count += 1
                continue
                
            # 1. Find account for this chat
            account_info = await get_account_session(chat_id)
            if not account_info:
                print(f"❌ No account found for Chat {chat_id}. Cannot re-download.")
                # Push back to DLQ
                r.rpush(DLQ_KEY, item_bytes)
                fail_count += 1
                continue
                
            session_path, phone = account_info
            
            # 2. Get/Connect Client
            if session_path not in clients:
                client = TelegramClient(
                    session_path,
                    settings.TG_API_ID,
                    settings.TG_API_HASH
                )
                await client.connect()
                if not await client.is_user_authorized():
                    print(f"❌ Account {phone} not authorized.")
                    clients[session_path] = None
                else:
                    clients[session_path] = client
            
            client = clients[session_path]
            if not client:
                r.rpush(DLQ_KEY, item_bytes)
                fail_count += 1
                continue
                
            # 3. Fetch Message & Media
            try:
                message = await client.get_messages(chat_id, ids=message_id)
                if not message or not (message.photo or message.video):
                    print(f"❌ Msg {message_id} in Chat {chat_id} not found or no media.")
                    # Don't push back? Or push back as 'permanent fail'?
                    # For now, drop it or push back. Let's drop it if it's gone.
                    fail_count += 1
                    continue
                    
                print(f"📥 Re-downloading media for Chat {chat_id} Msg {message_id}...")
                media_bytes = await client.download_media(message, file=bytes)
                
                if not media_bytes:
                    print("❌ Download returned empty.")
                    fail_count += 1
                    r.rpush(DLQ_KEY, item_bytes)
                    continue

                # 4. Construct Fresh Task
                # Re-encode content
                content_b64 = base64.b64encode(media_bytes).decode('ascii')
                
                # Update task data
                data['content_b64'] = content_b64
                if '_had_content' in data: del data['_had_content']
                if '_failure_reason' in data: del data['_failure_reason']
                
                # 5. Push to Main Queue
                r.rpush(MAIN_QUEUE_KEY, json.dumps(data))
                print(f"✅ Re-queued task for Chat {chat_id} Msg {message_id}")
                success_count += 1
                
            except Exception as e:
                print(f"❌ Error fetching/downloading: {e}")
                r.rpush(DLQ_KEY, item_bytes) # Push back
                fail_count += 1

        except Exception as e:
            print(f"Critical Loop Error: {e}")
            fail_count += 1
    
    # Disconnect all
    for client in clients.values():
        if client:
            await client.disconnect()
            
    print(f"\nSummary: {success_count} retried, {fail_count} failed/skipped.")

def main():
    parser = argparse.ArgumentParser(description="Manage Dead Letter Queue")
    parser.add_argument('action', choices=['inspect', 'clear', 'retry'], help='Action to perform')
    parser.add_argument('--limit', type=int, default=10, help='Limit for inspect/retry')
    
    args = parser.parse_args()
    
    if args.action == 'inspect':
        inspect_dlq(args.limit)
    elif args.action == 'clear':
        clear_dlq()
    elif args.action == 'retry':
        asyncio.run(retry_tasks(args.limit))

if __name__ == "__main__":
    main()
