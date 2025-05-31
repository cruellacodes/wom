import asyncio
import logging

from services.tweet_service import handle_on_demand_search

in_progress: dict[str, asyncio.Future] = {}

def get_or_create_token_future(token_symbol: str, queue: asyncio.Queue) -> asyncio.Future:
    loop = asyncio.get_event_loop()

    if token_symbol in in_progress:
        logging.info(f"[dedupe] Reusing in-progress search for {token_symbol}")
        return in_progress[token_symbol]

    future = loop.create_future()
    in_progress[token_symbol] = future
    queue.put_nowait((token_symbol, future))
    return future

async def process_search_queue(queue: asyncio.Queue):
    if queue.empty():
        return

    token_symbol, future = await queue.get()
    logging.info(f"[search_queue] Processing search for {token_symbol}")

    try:
        result = await handle_on_demand_search(token_symbol)
        if not future.done():
            future.set_result(result)
    except Exception as e:
        logging.error(f"[search_queue] Failed to process {token_symbol}: {e}")
        if not future.done():
            future.set_exception(e)
    finally:
        in_progress.pop(token_symbol, None)
