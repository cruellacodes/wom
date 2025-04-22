import asyncio
import os
import httpx
import logging
import pytz
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from dotenv import load_dotenv
from utils import is_relevant_tweet
from datetime import datetime, timedelta, timezone
from transformers import TextClassificationPipeline
from db import database
from models import tokens, tweets
from sqlalchemy.dialects.postgresql import insert

# Configure logging
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)

# Load environment variables
load_dotenv()

# Load CryptoBERT model
MODEL_NAME = "ElKulako/cryptobert"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)

# Initialize pipeline
pipe = TextClassificationPipeline(model=model, tokenizer=tokenizer, top_k=None)
    
async def store_tweets(token: str, processed_tweets: list):
    """Stores processed tweets in the PostgreSQL database."""
    if not processed_tweets:
        logging.info(f"No new tweets to store for {token}.")
        return

    logging.info(f"Attempting to store {len(processed_tweets)} tweets for {token}.")

    try:
        values = [
            {
                "tweet_id": tweet["tweet_id"],
                "token": token,
                "text": tweet["text"],
                "user_name": tweet["user_name"],
                "followers_count": tweet["followers_count"],
                "profile_pic": tweet["profile_pic"],
                "created_at": datetime.fromisoformat(tweet["created_at"]),
                "wom_score": tweet["wom_score"],
            }
            for tweet in processed_tweets
        ]

        stmt = insert(tweets).on_conflict_do_nothing(
            index_elements=["tweet_id"]
        )

        await database.execute_many(query=stmt, values=values)
        logging.info(f"Stored {len(processed_tweets)} tweets for {token}.")

    except Exception as e:
        logging.error(f"Error inserting tweets into PostgreSQL: {e}")
    
async def fetch_stored_tweets(token: str):
    """Fetch stored tweets for a specific token from PostgreSQL."""
    query = tweets.select().where(tweets.c.token == token).order_by(tweets.c.id.desc())
    rows = await database.fetch_all(query)

    return [
        {
            "id": row["id"],
            "text": row["text"],
            "user_name": row["user_name"],
            "followers_count": row["followers_count"],
            "profile_pic": row["profile_pic"],
            "created_at": row["created_at"],
            "wom_score": float(row["wom_score"]) if row["wom_score"] is not None else 1.0
        }
        for row in rows
    ]

async def fetch_tweets_from_rapidapi(token_symbol: str) -> list:
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    rapidapi_host = os.getenv("RAPIDAPI_HOST")

    if not rapidapi_key or not rapidapi_host:
        raise ValueError("RAPIDAPI_KEY or RAPIDAPI_HOST is missing in environment variables.")

    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": rapidapi_host
    }

    token_clean = token_symbol.replace("$", "").strip()
    search_term = f"${token_clean}" if len(token_clean) <= 6 else f"#{token_clean}"

    url = f"https://{rapidapi_host}/search.php"
    params = {
        "query": search_term,
        "search_type": "Latest"
    }

    logging.info(f"[RapidAPI] Fetching tweets for {search_term}")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            if not data.get("timeline"):
                logging.warning(f"[RapidAPI] No timeline data found for {token_symbol}. Full response: {data}")

            return data.get("timeline", [])

        except Exception as e:
            logging.error(f"[RapidAPI] Failed to fetch tweets for {token_symbol}: {e}")
            return []

async def analyze_sentiment(text):
    """Perform sentiment analysis on a tweet using CryptoBERT pipeline, returning a score from 0 to 2."""
    
    if not text:
        return 1.0  # Default neutral score if text is empty

    try:
        preds = pipe(text)[0]  # Get scores for Bearish, Neutral, Bullish

        # Compute score in range 0-2
        sentiment_score = round((1 * preds[1]['score']) + (2 * preds[2]['score']), 2)

        return sentiment_score 

    except Exception as e:
        logging.error(f"Sentiment analysis failed: {e}")
        return 1.0  # Default to neutral if CryptoBERT fails

async def get_sentiment(tweets_by_token):
    """Analyze sentiment for preprocessed tweets."""
    
    logging.debug(f"DEBUG: get_sentiment() received tweets_by_token: {type(tweets_by_token)} -> {tweets_by_token}")

    sentiment_results = {}

    if not isinstance(tweets_by_token, dict):
        logging.error(f"Unexpected data format for tweets_by_token: {type(tweets_by_token)}")
        return sentiment_results  # Return an empty dict instead of crashing

    for token, tweets in tweets_by_token.items():
        logging.debug(f"DEBUG: Processing {token}, tweets type: {type(tweets)}")
        
        if not isinstance(tweets, list):
            logging.error(f"Unexpected format for tweets[{token}]: {type(tweets)} -> {tweets}")
            continue

        if not tweets:
            logging.info(f"No tweets found for {token}. Default WOM Score applied.")
            sentiment_results[token] = {
                "wom_score": 1.0,  
                "tweet_count": 0,
                "tweets": []
            }
            continue
        
        logging.debug(f"DEBUG: {token} has {len(tweets)} tweets before sentiment analysis.")

        try:
            wom_scores = await asyncio.gather(
                *(analyze_sentiment(tweet.get("text", "")) for tweet in tweets)
            )
        except Exception as e:
            logging.error(f"Sentiment analysis failed for {token}: {e}")
            continue

        for i, tweet in enumerate(tweets):
            tweet["wom_score"] = wom_scores[i]

        avg_score = round((sum(wom_scores) / len(wom_scores)) / 2 * 100, 2) if wom_scores else 1.0

        sentiment_results[token] = {
            "wom_score": avg_score,  
            "tweet_count": len(tweets),
            "tweets": tweets
        }

    logging.debug(f"DEBUG: get_sentiment() returning: {sentiment_results}")
    return sentiment_results

async def fetch_tweet_volume_last_6h(token: str):
    """Return tweet count per hour for the last 6 hours for a given token"""
    current_time = datetime.now(timezone.utc)
    six_hours_ago = current_time - timedelta(hours=6)

    query = (
        tweets.select()
        .with_only_columns(tweets.c.created_at) 
        .where(
            (tweets.c.token == token) &
            (tweets.c.created_at >= six_hours_ago)
        )
    )

    rows = await database.fetch_all(query)

    # Prepare volume dictionary: Hour -6 to Hour -1
    tweet_volume = {f"Hour -{i}": 0 for i in range(6, 0, -1)}

    for row in rows:
        created_at = row["created_at"]
        
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at).replace(tzinfo=timezone.utc)

        hours_ago = int((current_time - created_at).total_seconds() // 3600)
        if 1 <= hours_ago <= 6:
            tweet_volume[f"Hour -{hours_ago}"] += 1

    return tweet_volume

async def fetch_and_analyze(token_symbol, store=True):
    """
    Fetch tweets, process them, analyze sentiment, and store (if needed) for a single token.

    Args:
        token_symbol (str): The token symbol to analyze.
        store (bool): Whether to store tweets in the DB (default: True).

    Returns:
        dict: Analysis results including tweets and sentiment data.
    """

    logging.info(f"Fetching and analyzing tweets for {token_symbol}...")

    # Step 1: Fetch tweets 
    raw_tweets = await fetch_tweets_from_rapidapi(token_symbol)

    if not raw_tweets:
        logging.error(f"No tweets found for {token_symbol}.")
        return {"token": token_symbol, "wom_score": 1.0, "tweet_count": 0, "tweets": []}

    # Step 2: Process tweets (filter and clean)
    processed_tweets_dict = await preprocess_tweets(raw_tweets, token_symbol)
    processed_tweets = processed_tweets_dict.get(token_symbol, [])

    if not processed_tweets:
        logging.error(f"All tweets filtered out for {token_symbol}.")
        return {"token": token_symbol, "wom_score": 1.0, "tweet_count": 0, "tweets": []}

    # Step 3: Analyze sentiment
    sentiment_dict = await get_sentiment(processed_tweets_dict)

    logging.debug(f"DEBUG: Sentiment analysis results: {sentiment_dict}")
    if not isinstance(sentiment_dict, dict) or token_symbol not in sentiment_dict:
            logging.error(f"Unexpected sentiment data format for {token_symbol}, setting default values.")
            return {"token": token_symbol, "wom_score": 1.0, "tweet_count": 0, "tweets": []}

    sentiment_data = sentiment_dict.get(token_symbol, {})

    if not isinstance(sentiment_data, dict):
        logging.error(f"Sentiment data for {token_symbol} is not a dictionary, setting default values.")
        return {"token": token_symbol, "wom_score": 1.0, "tweet_count": 0, "tweets": []}

    wom_score = float(sentiment_data.get("wom_score", 1.0))
    tweet_count = int(sentiment_data.get("tweet_count", 0))
    processed_tweets = sentiment_data.get("tweets", [])
    
    # Step 4: Store results if `store=True`
    if store:
        await store_tweets(token_symbol, processed_tweets)

        # Step 5: Update tokens table with WOM score and tweet count
        await update_token_data(token_symbol, wom_score, tweet_count)

    # Step 6: Return analysis results
    result = {
        "token": token_symbol,
        "wom_score": wom_score,
        "tweet_count": tweet_count,
        "tweets": processed_tweets,
    }
    
    logging.info(f"Completed analysis for {token_symbol}. WOM Score: {result['wom_score']}")
    return result

async def preprocess_tweets(raw_tweets, token_symbol, min_followers=150):
    """
    Filters and structures raw tweets.

    - Removes irrelevant tweets (e.g., spam, low engagement).
    - Extracts only useful fields.
    - Converts timestamps to UTC.

    Args:
        raw_tweets (list): List of raw tweet data.
        token_symbol (str): Token being analyzed.
        min_followers (int): Minimum followers required.

    Returns:
        dict: Processed tweets grouped under token symbol.
    """
    processed_tweets = []

    for tweet in raw_tweets:
        if tweet.get("type") != "tweet":
            continue

        user_info = tweet.get("user_info", {})

        tweet_data = {
            "id": tweet.get("tweet_id"),
            "text": tweet.get("text", "").strip(),
            "user_name": user_info.get("screen_name", ""),
            "followers_count": user_info.get("followers_count", 0),
            "profile_pic": user_info.get("avatar", ""),
            "created_at": tweet.get("created_at", ""),
        }

        # Convert timestamp
        try:
            dt = datetime.strptime(tweet_data["created_at"], "%a %b %d %H:%M:%S %z %Y")
            tweet_data["created_at"] = dt.astimezone(pytz.utc).isoformat()
        except Exception:
            tweet_data["created_at"] = None

        # Apply filtering criteria
        if is_relevant_tweet(tweet_data["text"]) and tweet_data["followers_count"] >= min_followers:
            processed_tweets.append(tweet_data)

    return {token_symbol: processed_tweets}

async def update_token_data(token_symbol: str, wom_score: float, tweet_count: int):
    """Updates the token table with WOM Score and tweet count using PostgreSQL upsert."""
    stmt = insert(tokens).values(
        token_symbol=token_symbol,
        wom_score=wom_score,
        tweet_count=tweet_count
    ).on_conflict_do_update(
        index_elements=["token_symbol"],
        set_={
            "wom_score": wom_score,
            "tweet_count": tweet_count
        }
    )

    try:
        await database.execute(stmt)
        logging.info(f"Updated tokens table: {token_symbol} -> WOM Score: {wom_score}, Tweet Count: {tweet_count}")
    except Exception as e:
        logging.error(f"Error updating token data for {token_symbol}: {e}")

async def get_active_tokens():
    query = tokens.select().where(tokens.c.is_active == True)
    rows = await database.fetch_all(query)
    return [row["token_symbol"] for row in rows]

async def run_tweet_pipeline():
    active_tokens = await get_active_tokens()
    tasks = [fetch_and_analyze(token) for token in active_tokens]
    await asyncio.gather(*tasks)