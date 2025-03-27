import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from twitter_analysis import fetch_and_analyze
from new_pairs_tracker import fetch_tokens
import sqlite3
from dotenv import load_dotenv
from main import init_db
import os

DISK_PATH = os.getenv("DISK_PATH", "/tmp")  # fallback for local/testing
os.makedirs(DISK_PATH, exist_ok=True)

DB_PATH = os.path.join(DISK_PATH, "tokens.db")

load_dotenv()

init_db()

def delete_old_tokens():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=48)
        cursor.execute("DELETE FROM tokens WHERE created_at <= ?", (cutoff_time,))
        conn.commit()
        conn.close()
        logging.info("Deleted tokens older than 48 hours.")
    except Exception as e:
        logging.error(f"Error deleting old tokens: {e}")

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
