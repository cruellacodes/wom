import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict

from services.tweet_service import fetch_last_Xh_tweets, preprocess_tweets

# Active volume fetches in progress
in_progress_volume: Dict[str, asyncio.Future] = {}

async def handle_volume_count(token_symbol: str) -> dict:
    raw_tweets = await fetch_last_Xh_tweets(token_symbol,24)
    processed = await preprocess_tweets(raw_tweets, token_symbol)
    tweets = processed.get(token_symbol, [])

    now = datetime.utcnow()
    filtered = [
        t for t in tweets
        if t.get("user", {}).get("followers_count", 0) >= 150
        and "created_at" in t
        and isinstance(t["created_at"], str)
        and datetime.fromisoformat(t["created_at"]) >= now - timedelta(hours=48)
    ]

    # Bucket by hour
    hourly_buckets = {}
    for t in filtered:
        dt = datetime.fromisoformat(t["created_at"]).replace(minute=0, second=0, microsecond=0)
        key = dt.isoformat()
        hourly_buckets[key] = hourly_buckets.get(key, 0) + 1

    return {
        "token": token_symbol,
        "total": len(filtered),
        "buckets": hourly_buckets,
    }

def get_or_create_volume_future(token_symbol: str, queue: asyncio.Queue) -> asyncio.Future:
    token_symbol = token_symbol.lower()
    loop = asyncio.get_event_loop()

    if token_symbol in in_progress_volume:
        logging.info(f"[volume] Reusing in-progress search for {token_symbol}")
        return in_progress_volume[token_symbol]

    future = loop.create_future()
    in_progress_volume[token_symbol] = future
    queue.put_nowait((token_symbol, future))
    return future

async def process_volume_queue(queue: asyncio.Queue):
    if queue.empty():
        return

    token_symbol, future = await queue.get()
    logging.info(f"[volume] Processing tweet volume for {token_symbol}")

    try:
        result = await handle_volume_count(token_symbol)
        if not future.done():
            future.set_result(result)
    except Exception as e:
        logging.error(f"[volume] Failed to process {token_symbol}: {e}")
        if not future.done():
            future.set_exception(e)
    finally:
        in_progress_volume.pop(token_symbol, None)
