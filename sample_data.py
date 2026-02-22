import asyncio
import selectors
from database import get_db_connection

async def get_sample():
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, topic_id, label FROM telegram_topics LIMIT 5")
                rows = await cur.fetchall()
                if not rows:
                    print("No topics found in database yet.")
                else:
                    print("Sample Data (DB ID | Telegram Thread ID | Label):")
                    for row in rows:
                        print(f"  {row[0]} | {row[1]} | {row[2]}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    selector = selectors.SelectSelector()
    loop = asyncio.SelectorEventLoop(selector)
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(get_sample())
    finally:
        loop.close()
