import asyncio
import itertools
import os
import aiosqlite
import httpx
import logging
import aiosqlite
import pytz
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
from dotenv import load_dotenv
from utils import is_relevant_tweet
from datetime import datetime, timedelta, timezone
import aiosqlite

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

async def store_tweets(token, processed_tweets, db_path):
    """
    Stores processed tweets in the database.

    Args:
        token (str): Token symbol.
        processed_tweets (list): List of filtered and structured tweets.
        db_path (str): Path to the database.

    Returns:
        None
    """
    if not processed_tweets:
        logging.info(f"No new tweets to store for {token}.")
        return

    async with aiosqlite.connect(db_path) as conn:
        cursor = await conn.cursor()

        new_tweets = [
            (tweet["id"], token, tweet["text"], tweet["user_name"],
             tweet["followers_count"], tweet["profile_pic"], tweet["created_at"], tweet["wom_score"])
            for tweet in processed_tweets
        ]

        try:
            await cursor.executemany("""
                INSERT OR IGNORE INTO tweets (id, token, text, user_name, followers_count, profile_pic, created_at, wom_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, new_tweets)
            await conn.commit()
            logging.info(f"Stored {len(new_tweets)} new relevant tweets for {token}.")
        except Exception as e:
            logging.error(f"Error inserting tweets into DB: {e}")


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
    
    
async def fetch_stored_tweets(token, db_path):
    """Fetch stored tweets for a specific token from the database."""
    async with aiosqlite.connect(db_path) as conn:
        cursor = await conn.cursor()
        await cursor.execute("SELECT id, text, user_name, followers_count, profile_pic, created_at, wom_score FROM tweets WHERE token = ? ORDER BY id DESC", (token,))
        rows = await cursor.fetchall()

    return [
        {
            "id": row[0],
            "text": row[1],
            "user_name": row[2],
            "followers_count": row[3],
            "profile_pic": row[4],
            "created_at": row[5],
            "wom_score": float(row[6]) 
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
    """Perform sentiment analysis on a tweet using CryptoBERT."""
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding="max_length", max_length=128)
    outputs = model(**inputs).logits
    scores = softmax(outputs.detach().numpy())[0]
    return scores[2]  # WOM Score



async def get_sentiment(tweets_by_token):
    """
    Analyze sentiment for preprocessed tweets.

    Args:
        tweets_by_token (dict): A dictionary where keys are token symbols and values are lists of processed tweets.

    Returns:
        dict: Sentiment scores per token, including per-tweet scores.
    """
    sentiment_results = {}

    for token, tweets in tweets_by_token.items():
        if not tweets:
            logging.info(f"No tweets found for {token}. Default WOM Score applied.")
            sentiment_results[token] = {"wom_score": 1.0, "tweet_count": 0, "tweets": []}
            continue

        logging.info(f"Processing {len(tweets)} tweets for {token}")

        # Run sentiment analysis concurrently
        try:
            wom_scores = await asyncio.gather(
                *(analyze_sentiment(tweet.get("text", "")) for tweet in tweets)
            )
        except Exception as e:
            logging.error(f"Sentiment analysis failed for {token}: {e}")
            continue

        wom_scores = [round(float(score) * 100, 2) for score in wom_scores]

        # Attach WOM score to each tweet
        for i, tweet in enumerate(tweets):
            tweet["wom_score"] = wom_scores[i]

        # Compute average WOM score
        avg_score = round(sum(wom_scores) / len(wom_scores) * 100, 2) if wom_scores else 0

        sentiment_results[token] = {
            "wom_score": avg_score,
            "tweet_count": len(tweets),
            "tweets": tweets, 
        }

    return sentiment_results



async def fetch_tweet_volume_last_6h(token, db_path):
    """Fetch stored tweets for a specific token and return tweet count per hour for the last 6 hours."""
    current_time = datetime.now(timezone.utc)  # Use timezone-aware UTC datetime

    # Create a dictionary to store tweet counts for each hour
    tweet_volume = {f"Hour -{i}": 0 for i in range(6, 0, -1)}

    async with aiosqlite.connect(db_path) as conn:
        cursor = await conn.cursor()

        # Fetch all tweets from the last 6 hours
        six_hours_ago = (current_time - timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")
        await cursor.execute(
            "SELECT created_at FROM tweets WHERE token = ? AND created_at >= ? ORDER BY created_at DESC",
            (token, six_hours_ago)
        )
        rows = await cursor.fetchall()

    # Count tweets per hour
    for row in rows:
        created_at_str = row[0]
        tweet_time = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        hours_ago = (current_time - tweet_time).seconds // 3600  # Calculate how many hours ago

        if 1 <= hours_ago <= 6:
            tweet_volume[f"Hour -{hours_ago}"] += 1  # Increment count for the respective hour

    return tweet_volume  # Returns { "Hour -6": X, "Hour -5": Y, ..., "Hour -1": Z }


async def fetch_and_analyze(token_symbol, store=True, db_path=None):
    """
    Fetch tweets, process them, analyze sentiment, and store (if needed) for a single token.

    Args:
        token_symbol (str): The token symbol to analyze.
        store (bool): Whether to store tweets in the DB (default: True).
        db_path (str): Path to the database.

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
    wom_score = float(sentiment_dict[token_symbol]["wom_score"])
    tweet_count = int(sentiment_dict[token_symbol]["tweet_count"])
    processed_tweets = sentiment_dict[token_symbol]["tweets"]

    # Step 4: Store results if `store=True`
    if store and db_path:
        await store_tweets(token_symbol, processed_tweets, db_path)

    # Step 5: Update tokens table with WOM score and tweet count
    await update_token_data(token_symbol, wom_score, tweet_count, db_path)

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
            tweet_data["created_at"] = dt.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            tweet_data["created_at"] = None

        # Apply filtering criteria
        if is_relevant_tweet(tweet_data["text"]) and tweet_data["followers_count"] >= min_followers:
            processed_tweets.append(tweet_data)

    return {token_symbol: processed_tweets}


async def update_token_data(token_symbol, wom_score, tweet_count, db_path):
    """
    Updates the token table with WOM Score and tweet count.

    Args:
        token_symbol (str): The token symbol to update.
        wom_score (float): The WOM score calculated from sentiment analysis.
        tweet_count (int): The number of tweets analyzed.
        db_path (str): Path to the SQLite database.
    """
    if not db_path:
        logging.warning("Database path not provided. Skipping token update.")
        return

    query = """
    INSERT INTO tokens (token_symbol, wom_score, tweet_count)
    VALUES (?, ?, ?)
    ON CONFLICT(token_symbol) DO UPDATE
    SET wom_score = excluded.wom_score, tweet_count = excluded.tweet_count;
    """

    async with aiosqlite.connect(db_path) as db:
        await db.execute(query, (token_symbol, wom_score, tweet_count))
        await db.commit()

    logging.info(f"Updated tokens table: {token_symbol} -> WOM Score: {wom_score}, Tweet Count: {tweet_count}")

