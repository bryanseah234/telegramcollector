#!/usr/bin/env python3
"""
Manage Queues
Allows you to:
1. REQUEUE failed tasks (Dead Letter Queue -> Main Queue)
2. CLEAR Dead Letter Queue (Delete failed tasks only)
3. CLEAR ALL Queues (Delete EVERYTHING - Data Loss)
"""
import redis
import os
import sys
import json

# Configuration (match docker-compose defaults if env vars missing)
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6380))  # External port
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

DLQ_KEY = "processing_queue:dead_letter"
QUEUE_KEY = "processing_queue:tasks"

def get_redis():
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=False # Keep as bytes for moving
        )
        r.ping()
        return r
    except Exception as e:
        print(f"❌ Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}")
        print(f"Error: {e}")
        return None

def show_stats(r):
    main_len = r.llen(QUEUE_KEY)
    dlq_len = r.llen(DLQ_KEY)
    print(f"\n📊 Queue Statistics:")
    print(f"   - Main Processing Queue: {main_len} tasks (Pending)")
    print(f"   - Dead Letter Queue:     {dlq_len} tasks (Failed)")
    return main_len, dlq_len

def requeue_dlq(r):
    """Moves all items from DLQ to Main Queue"""
    count = r.llen(DLQ_KEY)
    if count == 0:
        print("✅ Dead Letter Queue is empty. Nothing to retry.")
        return

    print(f"\n🔄 Moving {count} failed tasks back to Main Queue...")
    moved = 0
    while True:
        # Pop from DLQ (Left side - FIFO)
        item = r.lpop(DLQ_KEY)
        if not item:
            break
        
        try:
            data = json.loads(item)
            # Reset retry count so it has a fair chance
            if '_retry_count' in data:
                data['_retry_count'] = 0 
            if '_failure_reason' in data:
                del data['_failure_reason']
            
            # Add to back of main queue
            r.rpush(QUEUE_KEY, json.dumps(data))
            moved += 1
            
            if moved % 100 == 0:
                print(f"   Moved {moved}...")
        except Exception:
            # If parse fails, just push raw
            r.rpush(QUEUE_KEY, item)
            moved += 1

    print(f"✅ Successfully requeued {moved} tasks.")

def clear_dlq(r):
    """Deletes only DLQ"""
    count = r.llen(DLQ_KEY)
    if count == 0:
        print("✅ Dead Letter Queue is already empty.")
        return

    print(f"\n⚠️  Deleting {count} failed tasks from DLQ.")
    confirm = input(f"    Type 'DELETE' to confirm: ")
    
    if confirm.strip() == 'DELETE':
        r.delete(DLQ_KEY)
        print("🗑️  Dead Letter Queue CLEARED.")
    else:
        print("❌ Cancelled.")

def clear_all(r):
    """Deletes EVERYTHING"""
    main_len, dlq_len = show_stats(r)
    
    if main_len == 0 and dlq_len == 0:
        print("✅ Queues are already empty.")
        return

    print("\n💀 DANGER: This will delete ALL pending and failed tasks.")
    print("    Scanned messages will be skipped forever!")
    confirm = input(f"    Type 'NUKE' to confirm clearing {main_len + dlq_len} tasks: ")
    
    if confirm.strip() == 'NUKE':
        r.delete(QUEUE_KEY)
        r.delete(DLQ_KEY)
        print("💥 All queues CLEARED (Deleted).")
    else:
        print("❌ Cancelled.")

def main():
    print("🔧 Redis Queue Manager")
    print("====================")
    
    r = get_redis()
    if not r:
        sys.exit(1)

    while True:
        show_stats(r)
        print("\nOptions:")
        print("1. 🔄 Retry Failed Tasks (DLQ -> Main Queue)")
        print("2. 🗑️  Clear Dead Letter Queue (Delete Failed Only)")
        print("3. 💀 Clear EVERYTHING (Main + DLQ)")
        print("4. ❌ Exit")
        
        choice = input("\nSelect option [1]: ").strip() or "1"
        
        if choice == "1":
            requeue_dlq(r)
        elif choice == "2":
            clear_dlq(r)
        elif choice == "3":
            clear_all(r)
        elif choice == "4":
            break
        else:
            print("Invalid option.")

if __name__ == "__main__":
    main()
