import asyncio
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv
import logging
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
from contextlib import asynccontextmanager
from fastapi.responses import Response
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from new_pairs_tracker import fetch_tokens, fetch_and_analyze, fetch_tokens_from_db
import requests

logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)
load_dotenv()

# Create a global AsyncIOScheduler instance.
scheduler = AsyncIOScheduler()

# This job first fetches tokens and then runs tweet fetching/sentiment analysis.
async def scheduled_fetch():
    logging.info("Scheduled job started: Fetching tokens...")
    filtered_tokens = await fetch_tokens()
    delete_old_tokens()
    logging.info("Scheduled job continuing: Updating tokens and tweets...")
    await fetch_and_analyze(filtered_tokens)

# Schedule the job to run every 30 minutes.
scheduler.add_job(scheduled_fetch, 'interval', minutes=30)

DB_PATH = "tokens.db"

def init_db():
    """Initialize the database and create tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tweets (
            id TEXT PRIMARY KEY,
            token TEXT,
            text TEXT,
            followers_count INTEGER DEFAULT 0,
            user_name TEXT,
            profile_pic TEXT,
            created_at TEXT,
            wom_score REAL
        )
    """)
    # Updated tokens table to include wom_score and tweet_count.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tokens (
            token_symbol TEXT PRIMARY KEY,
            token_name TEXT,
            address TEXT,
            age_hours REAL,
            volume_usd REAL,
            maker_count INTEGER,
            liquidity_usd REAL,
            market_cap_usd REAL,
            dex_url TEXT,
            priceChange1h REAL,
            wom_score REAL,
            tweet_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    logging.info("Database (tokens) initialized successfully.")

def delete_old_tokens():
    """Delete tokens that are older than 24 hours."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get the timestamp of 24 hours ago
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
    cursor.execute("DELETE FROM tokens WHERE created_at <= ?", (cutoff_time,))
    conn.commit()
    conn.close()
    logging.info("Deleted old tokens created before %s.", cutoff_time)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Starting FastAPI App...")
    init_db()  # Initialize the database.
    loop = asyncio.get_running_loop()
    scheduler.configure(event_loop=loop)
    scheduler.start()
    logging.info("Scheduler started.")
    try:
        yield
    finally:
        logging.info("Shutting down FastAPI App...")
        scheduler.shutdown()

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
def fetch_tokens_from_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT token_symbol, age_hours, volume_usd, maker_count,
               liquidity_usd, market_cap_usd, dex_url, priceChange1h, wom_score, tweet_count
        FROM tokens
    """)
    rows = cursor.fetchall()
    conn.close()
    tokens = []
    for row in rows:
        tokens.append({
            "Token": row[0],
            "Age": row[1],
            "Volume": row[2],
            "MakerCount": row[3],
            "Liquidity": row[4],
            "MarketCap": row[5],
            "dex_url": row[6],
            "priceChange1h": row[7],
            "WomScore": row[8],
            "TweetCount":row[9]
        })
    return tokens

# Endpoint to fetch token details from the database.
@app.get("/tokens")
async def get_tokens_details():
    try:
        tokens = fetch_tokens_from_db()
        if not tokens:
            logging.info("No tokens available in the database.")
            return {"message": "No tokens available"}
        return tokens
    except Exception as e:
        logging.error(f"Error fetching token details: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/stored-tweets/")
async def get_stored_tweets_endpoint(token: str = Query(..., description="Token symbol")):
    try:
        from twitter_analysis import fetch_stored_tweets
        tweets = await fetch_stored_tweets(token, DB_PATH)
        if not tweets:
            return {"message": f"No stored tweets found for {token}"}
        return {"token": token, "tweets": tweets}
    except Exception as e:
        logging.error(f"Error fetching stored tweets for {token}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
@app.get("/tweet-volume/")
async def get_tweet_volume_endpoint(token: str = Query(..., description="Token symbol")):
    try:
        from twitter_analysis import fetch_tweet_volume_last_6h
        tweet_volume = await fetch_tweet_volume_last_6h(token, DB_PATH)
        return {"token": token, "tweet_volume": tweet_volume}
    except Exception as e:
        logging.error(f"Error fetching tweet volume for {token}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# Endpoint to manually trigger token fetching.
@app.get("/trigger-fetch")
async def trigger_fetch():
    tokens = await fetch_tokens()
    await fetch_and_analyze(tokens)
    return {"message": "Fetch triggered manually", "tokens": tokens}


DEX_SCREENER_TOKEN_API = "https://api.dexscreener.com/tokens/v1"

@app.get("/search-token/{chain_id}/{token_address}")  
async def search_token(chain_id: str, token_address: str):
    """
    Fetch token details from Dex Screener API based on token address and chain ID.
    """
    try:
        url = f"{DEX_SCREENER_TOKEN_API}/{chain_id}/{token_address}" 

        response = requests.get(url)

        if response.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to fetch token data")

        data = response.json()

        if not isinstance(data, list) or len(data) == 0:
            raise HTTPException(status_code=404, detail="Token not found")

        # Extract the first matching token
        token_data = data[0]

        # Calculate token age in hours
        pair_created_timestamp = token_data.get("pairCreatedAt", 0)
        if pair_created_timestamp:
            token_age_hours = (datetime.now(timezone.utc).timestamp() - (pair_created_timestamp / 1000)) / 3600
            token_age_hours = round(token_age_hours, 2)  # Round to 2 decimal places
        else:
            token_age_hours = "N/A"

        return {
            "symbol": token_data["baseToken"]["symbol"],
            "priceUsd": token_data["priceUsd"],
            "marketCap": token_data.get("marketCap", "N/A"),
            "liquidity": token_data["liquidity"]["usd"],
            "volume24h": token_data["volume"].get("h24", 0),  # 24-hour volume
            "priceChange1h": token_data["priceChange"].get("h1", 0),  # 1-hour price change
            "ageHours": token_age_hours,  # Token age in hours
            "dexUrl": token_data["url"]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    tokens_with_sentiment = asyncio.run(fetch_and_analyze())
    print(tokens_with_sentiment)
