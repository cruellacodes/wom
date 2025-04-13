from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Query, Header, BackgroundTasks
from dotenv import load_dotenv
import logging
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from fastapi.responses import Response
from sqlalchemy import create_engine
from twitter_analysis import fetch_and_analyze, fetch_stored_tweets, fetch_tweet_volume_last_6h
from new_pairs_tracker import fetch_tokens
import requests
import os
import random
from db import DATABASE_URL, database, sa_metadata
from models import tokens
import time

BROWSER_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...",
    ...
]

load_dotenv()

def create_tables():
    engine = create_engine(DATABASE_URL)
    sa_metadata.create_all(engine)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Starting FastAPI App...")

    create_tables()

    await database.connect()
    logging.info("Connected to PostgreSQL database.")
    try:
        yield
    finally:
        await database.disconnect()
        logging.info("Disconnected from PostgreSQL.")

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/favicon.ico", include_in_schema=False)
async def ignore_favicon():
    return Response(status_code=204)

# Helper function to fetch tokens from DB including sentiment data.
async def fetch_tokens_from_db():
    query = tokens.select()
    rows = await database.fetch_all(query)

    return [
        {
            "Token": row["token_symbol"],
            "Age": row["age_hours"],
            "Volume": row["volume_usd"],
            "MakerCount": row["maker_count"],
            "Liquidity": row["liquidity_usd"],
            "MarketCap": row["market_cap_usd"],
            "dex_url": row["dex_url"],
            "priceChange1h": row["pricechange1h"],
            "WomScore": row["wom_score"],
            "TweetCount": row["tweet_count"]
        }
        for row in rows
    ]

@app.get("/tokens")
async def get_tokens():
    print("Received request at /tokens")
    start = time.time()
    result = await fetch_tokens_from_db()
    print(f"Fetched {len(result)} tokens in {time.time() - start:.2f}s")
    return result

# Endpoint to fetch token details from the database.
#@app.get("/tokens")
#async def get_tokens_details():
#    try:
#        tokens = await fetch_tokens_from_db()
#        if not tokens:
#            logging.info("No tokens available in the database.")
#            return {"message": "No tokens available"}
#        return tokens
#    except Exception as e:
#        logging.error(f"Error fetching token details: {e}")
#        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/stored-tweets/")
async def get_stored_tweets_endpoint(token: str = Query(..., description="Token symbol")):
    try:
        tweets = await fetch_stored_tweets(token)
        if not tweets:
            return {"message": f"No stored tweets found for {token}"}
        return {"token": token, "tweets": tweets}
    except Exception as e:
        logging.error(f"Error fetching stored tweets for {token}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
@app.get("/tweet-volume/")
async def get_tweet_volume_endpoint(token: str = Query(..., description="Token symbol")):
    try:
        tweet_volume = await fetch_tweet_volume_last_6h(token)
        return {"token": token, "tweet_volume": tweet_volume}
    except Exception as e:
        logging.error(f"Error fetching tweet volume for {token}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/run-scheduled-job")
async def run_scheduled_job(key: str = Header(...), background_tasks: BackgroundTasks = None):
    expected_key = os.getenv("SCHEDULE_KEY")
    if key != expected_key:
        raise HTTPException(status_code=403, detail="Forbidden")
    
    # Schedule the heavy task to run in the background
    background_tasks.add_task(process_tokens)
    
    return {"message": "Job triggered"}

async def process_tokens():
    try:
        tokens = await fetch_tokens()
        results = []
        for token in tokens:
            result = await fetch_and_analyze(token["token_symbol"])
            results.append(result)
        logging.info(f"Scheduled job completed. {len(results)} tokens processed.")
    except Exception as e:
        logging.error(f"Error in scheduled job: {e}")


@app.get("/search-token/{chain_id}/{token_address}")
async def search_token(chain_id: str, token_address: str):
    try:
        url = f"https://search-wom.cruellacodes.workers.dev/{chain_id}/{token_address}"

        headers = {
            "User-Agent": random.choice(BROWSER_USER_AGENTS),
            "Accept": "application/json",
            "Accept-Encoding": "identity",  # avoids gzip, br
        }

        response = requests.get(url, headers=headers)

        try:
            data = response.json()
            logging.info("[Dex API] JSON parsed successfully")
        except Exception as e:
            logging.error(f"[Dex API] Failed to parse JSON: {e}")
            logging.debug(f"[Dex API] Raw content: {response.content[:500]}")
            raise


        data = response.json()

        if not isinstance(data, list) or len(data) == 0:
            raise HTTPException(status_code=404, detail="Token not found")

        token_data = data[0]  # The actual token object
        token_symbol = token_data["baseToken"]["symbol"]

        pair_created_timestamp = token_data.get("pairCreatedAt", 0)
        token_age_hours = round(
            (datetime.now(timezone.utc).timestamp() - (pair_created_timestamp / 1000)) / 3600, 2
        ) if pair_created_timestamp else "N/A"

        return {
            "symbol": token_symbol,
            "priceUsd": token_data.get("priceUsd", "N/A"),
            "marketCap": token_data.get("marketCap", "N/A"),
            "liquidity": token_data.get("liquidity", {}).get("usd", 0),
            "volume24h": token_data.get("volume", {}).get("h24", 0),
            "priceChange1h": token_data.get("priceChange", {}).get("h1", 0),
            "ageHours": token_age_hours,
            "dexUrl": token_data.get("url", "#"),
        }

    except Exception as e:
        logging.error(f"Error in search_token: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.get("/tweets/{token_symbol}")
async def get_tweets(token_symbol: str):
    """
    Fetch tweets **on demand** without storing them.
    """
    try:
        tweets_data = await fetch_and_analyze(token_symbol)
        
        formatted_tweets = [
            {
                "id": tweet.get("id"),
                "text": tweet.get("text"),
                "user_name": tweet.get("user_name"),
                "followers_count": tweet.get("followers_count", 0),
                "profile_pic": tweet.get("profile_pic", ""),
                "created_at": tweet.get("created_at", ""),
                "wom_score": tweet.get("wom_score", 1.0),
            }
            for tweet in tweets_data.get("tweets", []) 
        ]

        return {
            "token": token_symbol, 
            "tweets": formatted_tweets,
            "wom_score": tweets_data.get("wom_score", 0)
        }
    
    except Exception as e:
        logging.error(f"Error fetching tweets for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
