from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from db import database
from contextlib import asynccontextmanager
import logging
import asyncio
from services.token_service import fetch_tokens
from services.tweet_service import run_tweet_pipeline
from routes.tokens import tokens_router
from routes.tweets import tweets_router
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    level=logging.INFO
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    logging.info("Connected to database.")

    async def schedule_token_fetch():
        while True:
            start = datetime.now(timezone.utc)
            try:
                await fetch_tokens()
            except Exception as e:
                logging.error(f"[fetch_tokens error] {e}")
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            await asyncio.sleep(max(0, 300 - elapsed))


    async def schedule_tweet_fetch():
        while True:
            start = datetime.now(timezone.utc)
            logging.info("Fetching tweets for all active tokens...")
            try:
                await run_tweet_pipeline()
            except Exception as e:
                logging.error(f"[tweet_pipeline error] {e}")
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            await asyncio.sleep(max(0, 60 - elapsed))


    # Background tasks
    token_task = asyncio.create_task(schedule_token_fetch())
    tweet_task = asyncio.create_task(schedule_tweet_fetch())

    try:
        yield
    finally:
        logging.info(" Shutting down background tasks...")
        token_task.cancel()
        tweet_task.cancel()
        await database.disconnect()
        logging.info(" Disconnected from database.")

app = FastAPI(lifespan=lifespan)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check
@app.get("/health")
async def health_check():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

# Routes
app.include_router(tokens_router)
app.include_router(tweets_router)
