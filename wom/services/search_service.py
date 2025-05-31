import asyncio
import logging
from services.tweet_service import handle_on_demand_search

async def process_search_queue(queue: asyncio.Queue):
    if queue.empty():
        return

    token_symbol, future = await queue.get()

    try:
        result = await handle_on_demand_search(token_symbol)
        future.set_result(result)
    except Exception as e:
        logging.error(f"[search_queue] Failed to process {token_symbol}: {e}")
        future.set_exception(e)
