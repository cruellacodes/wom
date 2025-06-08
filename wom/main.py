import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db import database
from services.token_service import (
    fetch_tokens,
    deactivate_low_activity_tokens,
    delete_old_tokens,
)
from services.tweet_service import run_tweet_pipeline, run_score_pipeline
from services.tweet_service import prune_old_tweets
from routes.tokens import tokens_router
from routes.tweets import tweets_router

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s", level=logging.INFO
)

def make_loop(fn, interval_seconds):
    """Spawn a background task that runs `await fn()` every interval_seconds."""
    async def _loop():
        logging.info(f"Loop started: {fn.__name__} every {interval_seconds}s")
        while True:
            start = datetime.now(timezone.utc)
            try:
                await fn()
            except Exception as exc:
                logging.error(f"[{fn.__name__}] Error: {exc}", exc_info=True)
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            logging.debug(f"[{fn.__name__}] Took {elapsed:.2f}s")
            await asyncio.sleep(max(0, interval_seconds - elapsed))
    task = asyncio.create_task(_loop())
    
    # Add crash monitoring
    def handle_crash(task):
        try:
            task.result()
        except asyncio.CancelledError:
            logging.info(f"[{fn.__name__}] Task was cancelled.")
        except Exception as e:
            logging.critical(f"[{fn.__name__}] Task crashed fatally: {e}", exc_info=True)

    task.add_done_callback(handle_crash)
    return task


@asynccontextmanager
async def lifespan(app: FastAPI):
    from services.search_service import process_search_queue 

    # Connect to DB
    await database.connect()
    logging.info("Connected to database.")

    # Create in-memory async queue
    search_queue = asyncio.Queue(maxsize=100)  
    app.state.search_queue = search_queue

    # Start background workers
    tasks = [
        make_loop(fetch_tokens, 1800),
        make_loop(tweet_score_deactivate_pipeline, 120),
        make_loop(prune_old_tweets, 1800), 
    ]

    # Launch 5 parallel search processors
    for i in range(5):
        tasks.append(make_loop(lambda: process_search_queue(search_queue), 1))

    yield

    logging.info("Shutting down background tasks…")
    for t in tasks:
        t.cancel()
    await database.disconnect()
    logging.info("Disconnected from database.")

app = FastAPI(lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"],   allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

async def tweet_score_deactivate_pipeline():
    await run_tweet_pipeline()
    await run_score_pipeline()
    await deactivate_low_activity_tokens()

app.include_router(tokens_router)
app.include_router(tweets_router)
