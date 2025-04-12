import asyncio
import itertools
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
from models import tweets

# Configure logging
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)

# Load environment variables
load_dotenv()

# Initialize Apify API token & task IDs
api_token = os.getenv("APIFY_API_TOKEN")
task_ids_str = os.getenv("WORKER_IDS")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")
if not task_ids_str:
    raise ValueError("WORKER_IDS not set in environment variables!")

# Parse task IDs
task_ids = [tid.strip() for tid in task_ids_str.split(",") if tid.strip()]

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
        query = """
        INSERT INTO tweets (
            id, token, text, user_name, followers_count, profile_pic, created_at, wom_score
        )
        VALUES (
            :id, :token, :text, :user_name, :followers_count, :profile_pic, :created_at, :wom_score
        )
        ON CONFLICT (id) DO UPDATE SET
            token = excluded.token,
            text = excluded.text,
            user_name = excluded.user_name,
            followers_count = excluded.followers_count,
            profile_pic = excluded.profile_pic,
            created_at = excluded.created_at,
            wom_score = excluded.wom_score;
        """

        values = [
            {
                "id": tweet["id"],
                "token": token,
                "text": tweet["text"],
                "user_name": tweet["user_name"],
                "followers_count": tweet["followers_count"],
                "profile_pic": tweet["profile_pic"],
                "created_at": tweet["created_at"],  # Must be ISO 8601 string or datetime object
                "wom_score": tweet["wom_score"],
            }
            for tweet in processed_tweets
        ]

        await database.execute_many(query=query, values=values)
        logging.info(f"Stored {len(processed_tweets)} tweets for {token}.")

    except Exception as e:
        logging.error(f"Error inserting tweets into PostgreSQL: {e}")


async def update_task_input(task_id, new_input):
    """
    Update the task input using a direct HTTP PUT request.
    
    Args:
        task_id (str): The Apify task ID.
        new_input (dict): The new input for the task.
        
    Returns:
        dict: The updated task input as returned by the Apify API.
    """
    url = f"https://api.apify.com/v2/actor-tasks/{task_id}/input"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    async with httpx.AsyncClient() as client:
        response = await client.put(url, headers=headers, json=new_input)
        response.raise_for_status()
        return response.json()
    
    
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

async def fetch_tweets(token, task_id):
    """
    Fetch tweets using Apify. If `store=True`, store them in the DB.
    Otherwise, return the tweets without storing.
    """
    search_value = token.lower().replace("$", "")
    search_term = f"${search_value}" if len(token) <= 6 else f"#{search_value}"

    new_input = {
        "searchTerms": [search_term],
        "sortBy": "Latest",
        "maxItems": 50,
        "minRetweets": 0,
        "minLikes": 0,
        "minReplies": 0,
        "tweetLanguage": "en"
    }

    try:
        logging.info(f"Updating task input for {token} using task {task_id}...")
        await update_task_input(task_id, new_input)

        await asyncio.sleep(5)

        async with httpx.AsyncClient() as client:
            run_response = await client.post(
                f"https://api.apify.com/v2/actor-tasks/{task_id}/runs?token={api_token}"
            )
            run_response.raise_for_status()
            run_id = run_response.json()["data"]["id"]

            while True:
                await asyncio.sleep(7)
                run_status = await client.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}?token={api_token}"
                )
                run_status.raise_for_status()
                status = run_status.json()["data"]["status"]
                if status == "SUCCEEDED":
                    break
                elif status in ["FAILED", "TIMED_OUT", "ABORTED"]:
                    raise RuntimeError(f"Apify run failed with status: {status}")

            dataset_id = run_status.json()["data"]["defaultDatasetId"]
            dataset_response = await client.get(
                f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={api_token}"
            )
            dataset_response.raise_for_status()
            fetched_tweets = dataset_response.json()
        if fetched_tweets:
            logging.info(f"New tweets found: {len(fetched_tweets)} for {token}")

        return fetched_tweets

    except Exception as e:
        logging.error(f"Error fetching tweets for {token} using task {task_id}: {e}")
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

    # Step 1: Fetch tweets (DO NOT store yet)
    task_cycle = itertools.cycle(task_ids)
    raw_tweets = await fetch_tweets(token_symbol, next(task_cycle))

    if not raw_tweets:
        logging.info(f"No tweets found for {token_symbol}.")
        return {"token": token_symbol, "wom_score": 1.0, "tweet_count": 0, "tweets": []}

    # Step 2: Process tweets (filter and clean)
    processed_tweets_dict = await preprocess_tweets(raw_tweets, token_symbol)
    processed_tweets = processed_tweets_dict.get(token_symbol, [])

    if not processed_tweets:
        logging.info(f"All tweets filtered out for {token_symbol}.")
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
        min_followers (int): Minimum followers required.

    Returns:
        list: Processed tweets.
    """
    processed_tweets = []
    for tweet in raw_tweets:
        tweet_data = {
            "id": tweet.get("id"),
            "text": tweet.get("fullText", "").strip(),
            "user_name": tweet.get("author", {}).get("userName", ""),
            "followers_count": tweet.get("author", {}).get("followers", 0),
            "profile_pic": tweet.get("author", {}).get("profilePicture", ""),
            "created_at": tweet.get("createdAt", ""),
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
    """Updates the token table with WOM Score and tweet count in PostgreSQL."""
    query = """
    INSERT INTO tokens (token_symbol, wom_score, tweet_count)
    VALUES (:token_symbol, :wom_score, :tweet_count)
    ON CONFLICT (token_symbol) DO UPDATE SET
        wom_score = excluded.wom_score,
        tweet_count = excluded.tweet_count
    """

    values = {
        "token_symbol": token_symbol,
        "wom_score": wom_score,
        "tweet_count": tweet_count
    }

    try:
        await database.execute(query=query, values=values)
        logging.info(f"Updated tokens table: {token_symbol} -> WOM Score: {wom_score}, Tweet Count: {tweet_count}")
    except Exception as e:
        logging.error(f"Error updating token data for {token_symbol}: {e}")