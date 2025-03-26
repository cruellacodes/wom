import asyncio
import logging
from datetime import datetime, timedelta, timezone
from twitter_analysis import fetch_and_analyze
from new_pairs_tracker import fetch_tokens
import sqlite3
from dotenv import load_dotenv

load_dotenv()

DB_PATH = "tokens.db"

def delete_old_tokens():
    """Delete tokens that are older than 24 hours."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
    cursor.execute("DELETE FROM tokens WHERE created_at <= ?", (cutoff_time,))
    conn.commit()
    conn.close()
    logging.info("Deleted old tokens created before %s.", cutoff_time)

async def scheduled_job():
    logging.info("Cron job started: Fetching tokens and analyzing tweets...")
    tokens = await fetch_tokens()
    delete_old_tokens()

    results = []
    for token in tokens:
        result = await fetch_and_analyze(token["token_symbol"], store=True, db_path=DB_PATH)
        results.append(result)

    logging.info(f"Cron job finished. Processed {len(results)} tokens.")

if __name__ == "__main__":
    asyncio.run(scheduled_job())
