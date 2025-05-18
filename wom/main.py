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
from services.tweet_service import run_tweet_pipeline
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
            logging.critical(f"[{fn.__name__}] 💀 Task crashed fatally: {e}", exc_info=True)

    task.add_done_callback(handle_crash)
    return task


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1) DB connect
    await database.connect()
    logging.info("Connected to database.")

    # 2) Kick off ALL 4 loops as tasks
    tasks = [
        make_loop(fetch_tokens,            300),  # every 5m
        make_loop(run_tweet_pipeline,       60),  # every 1m
        make_loop(deactivate_low_activity_tokens, 60),  # every 1m
        # make_loop(delete_old_tokens,        60),  # every 1m
    ]

    yield  

    # 3) Teardown
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

app.include_router(tokens_router)
app.include_router(tweets_router)
