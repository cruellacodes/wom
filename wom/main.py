from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from db import database
from contextlib import asynccontextmanager
import logging
import asyncio
from services.token_service import fetch_tokens
from routes.tokens import tokens_router
from routes.tweets import tweets_router
from services.tweet_service import run_tweet_pipeline

@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    
    async def schedule_token_fetch():
        while True:
            try:
                await fetch_tokens()
            except Exception as e:
                logging.error(f"[fetch_tokens error] {e}")
            await asyncio.sleep(300)

    async def schedule_tweet_fetch():
        while True:
            logging.info("Fetching tweets for all active tokens...")
            try:
                await run_tweet_pipeline()
            except Exception as e:
                logging.error(f"[tweet_pipeline error] {e}")
            await asyncio.sleep(60)

    # Launch both background loops
    asyncio.create_task(schedule_token_fetch())
    asyncio.create_task(schedule_tweet_fetch())

    try:
        yield
    finally:
        await database.disconnect()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health
@app.get("/health")
async def health_check():
    return {"status": "ok"}

# Routers
app.include_router(tokens_router)
app.include_router(tweets_router)

async def background_loop():
    while True:
        logging.info("Fetching tweets for all active tokens...")
        await run_tweet_pipeline()
        await asyncio.sleep(60)
