import asyncio
import itertools
from fastapi import FastAPI, HTTPException
import os
from dotenv import load_dotenv
from new_pairs_tracker import get_filtered_pairs
from twitter_analysis import get_sentiment
from twitter_analysis import fetch_stored_tweets
from twitter_analysis import fetch_tweets
import logging
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
from contextlib import asynccontextmanager
from fastapi.responses import Response
from fastapi import Query

# Configure logging
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)

# Load environment variables
load_dotenv()
api_token = os.getenv("APIFY_API_TOKEN")
task_ids_str = os.getenv("WORKER_IDS")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")
if not task_ids_str:
    raise ValueError("WORKER_IDS not set in environment variables!")

# Parse task IDs
task_ids = [tid.strip() for tid in task_ids_str.split(",") if tid.strip()]

DB_PATH = "tweets.db"

def init_db():
    """Initialize the database and create tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tweets (
            id TEXT PRIMARY KEY,
            token TEXT,
            text TEXT,
            user_name TEXT,
            profile_pic TEXT
        )
    """)
    
    conn.commit()
    conn.close()
    print("Database initialized successfully.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan event to initialize the database."""
    logging.info("Starting FastAPI App...")
    init_db()  # Initialize DB at startup
    yield
    logging.info("Shutting down FastAPI App...")

# Initialize FastAPI with lifespan
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins (use ["http://localhost:5173"] for more security)
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

@app.get("/favicon.ico", include_in_schema=False)
async def ignore_favicon():
    return Response(status_code=204)

async def fetch_and_analyze():
    """
    Fetch new tokens, tweets, store them, and analyze sentiment from stored tweets.

    Returns:
        List[dict]: Each dictionary contains:
            - Token
            - WomScore (sentiment percentage)
            - TweetCount (total tweets stored in DB for the token)
            - MarketCap
            - Age
            - Volume
            - MakerCount
            - Liquidity
    """
    logging.info("Fetching filtered token pairs...")
    tokens = await get_filtered_pairs()  # Fetch token list
    if not tokens:
        logging.info("No tokens found matching the criteria.")
        return []

    cashtags = [token['token_symbol'] for token in tokens]
    logging.info(f"Tokens for sentiment analysis: {cashtags}")

    #Fetch new tweets and store them in the database
    task_cycle = itertools.cycle(task_ids)
    tasks = [fetch_tweets(token, next(task_cycle), DB_PATH) for token in cashtags]
    await asyncio.gather(*tasks)  # Run all fetch tasks in parallel

    #Analyze sentiment from stored tweets
    sentiment_dict = await get_sentiment(cashtags, DB_PATH)

    # Merge token data with sentiment analysis results
    final_results = []
    for token in tokens:
        ts = token.get("token_symbol")

        wom_score = sentiment_dict.get(ts, {}).get("wom_score", 0)  # Sentiment Score
        tweet_count = sentiment_dict.get(ts, {}).get("tweet_count", 0)  # Total stored tweets

        wom_score = float(wom_score) if wom_score is not None else None

        result = {
            "Token": ts,
            "WomScore": wom_score,
            "TweetCount": tweet_count,
            "MarketCap": token.get("market_cap_usd"),
            "Age": token.get("age_hours"),
            "Volume": token.get("volume_usd"),
            "MakerCount": token.get("maker_count"),
            "Liquidity": token.get("liquidity_usd"),
        }
        final_results.append(result)

    return final_results



@app.get("/tokens")
async def get_token_sentiment():
    """
    API Endpoint to fetch and return token information combined with sentiment analysis.
    
    Returns:
        JSON: List of token dictionaries.
    """
    try:
        results = await fetch_and_analyze()
        if not results:
            logging.info("No data available")
            return {"message": "No data available"}
        
        logging.info("Token Sentiment Analysis Results:")
        logging.info(results)
        return results
    except Exception as e:
        logging.error(f"Error in get_token_sentiment: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.get("/stored-tweets/")
async def get_stored_tweets(token: str = Query(..., description="Token symbol")):
    """
    API Endpoint to fetch stored tweets for a specific token.

    Args:
        token (str): Token symbol (e.g., $ETH, $BTC).

    Returns:
        JSON: List of stored tweets.
    """
    try:
        tweets = await fetch_stored_tweets(token, DB_PATH)
        if not tweets:
            return {"message": f"No stored tweets found for {token}"}
        return {"token": token, "tweets": tweets}
    except Exception as e:
        logging.error(f"Error fetching stored tweets for {token}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")