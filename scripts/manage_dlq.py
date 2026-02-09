#!/usr/bin/env python3
"""
Manage Dead Letter Queue (DLQ)
View, retry, or purge failed tasks from Redis.
"""
import redis
import json
import os
import sys
import argparse
from datetime import datetime

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
            decode_responses=True
        )
        r.ping()
        return r
    except Exception as e:
        print(f"❌ Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}")
        print(f"Error: {e}")
        return None

def view_dlq(r, limit=10):
    count = r.llen(DLQ_KEY)
    print(f"\n📉 Dead Letter Queue Size: {count}")
    
    if count == 0:
        return

    print(f"\nLast {limit} failed tasks:")
    print("-" * 60)
    
    # Peek at last N items
    items = r.lrange(DLQ_KEY, -limit, -1)
    
    for i, item in enumerate(items):
        try:
            data = json.loads(item)
            reason = data.get('_failure_reason', 'Unknown')
            timestamp = data.get('_failed_at', 'Unknown')
            chat_id = data.get('chat_id', 'N/A')
            msg_id = data.get('message_id', 'N/A')
            task_type = data.get('task_type', 'N/A')
            
            print(f"[{i+1}] {timestamp} | Type: {task_type}")
            print(f"    Chat: {chat_id} | Msg: {msg_id}")
            print(f"    Reason: {reason}")
            print("-" * 60)
        except:
            print(f"[{i+1}] Invalid JSON data")

def retry_all(r):
    count = r.llen(DLQ_KEY)
    if count == 0:
        print("DLQ is empty.")
        return

    print(f"Select retry mode:")
    print("1. Re-queue all items (fifo)")
    print("2. Re-queue specific failure type")
    choice = input("Choice [1]: ").strip() or "1"
    
    moved = 0
    
    if choice == "1":
        while True:
            item = r.lpop(DLQ_KEY)
            if not item:
                break
            r.rpush(QUEUE_KEY, item)
            moved += 1
    
    print(f"✅ Moved {moved} tasks back to main queue.")

def purge_dlq(r):
    count = r.llen(DLQ_KEY)
    if count == 0:
        print("DLQ is empty.")
        return
        
    confirm = input(f"⚠️ Are you sure you want to DELETE {count} failed tasks? (y/N): ")
    if confirm.lower() == 'y':
        r.delete(DLQ_KEY)
        print("🗑️ DLQ purged.")
    else:
        print("Cancelled.")

def main():
    print("💀 Dead Letter Queue Manager")
    print("============================")
    
    r = get_redis()
    if not r:
        sys.exit(1)

    while True:
        print("\nOptions:")
        print("1. View recent failures")
        print("2. Retry tasks (move to main queue)")
        print("3. Purge DLQ (delete all)")
        print("4. Exit")
        
        choice = input("\nSelect option: ").strip()
        
        if choice == "1":
            view_dlq(r)
        elif choice == "2":
            retry_all(r)
        elif choice == "3":
            purge_dlq(r)
        elif choice == "4":
            break
        else:
            print("Invalid option")

if __name__ == "__main__":
    main()
