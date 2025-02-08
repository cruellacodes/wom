import asyncio
import itertools
import os
import httpx
import logging
import aiosqlite
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
from dotenv import load_dotenv
from utils import preprocess_tweet, is_relevant_tweet

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


async def store_tweets(token, tweets, db_path):
    """Store only new tweets for a token in the database asynchronously."""
    if not tweets:
        logging.info(f"No new tweets to store for {token}.")
        return

    async with aiosqlite.connect(db_path) as conn:
        cursor = await conn.cursor()

        new_tweets = []
        for tweet in tweets:
            tweet_id = tweet.get("id")
            text = tweet.get("fullText", "").strip()
            user_name = tweet.get("author", {}).get("userName", "")
            profile_pic = tweet.get("author", {}).get("profilePicture", "")

            # Check if the tweet ID already exists
            await cursor.execute("SELECT id FROM tweets WHERE id = ?", (tweet_id,))
            existing = await cursor.fetchone()

            if not existing:
                new_tweets.append((tweet_id, token, text, user_name, profile_pic))

        # Insert only new tweets and ignore duplicates
        if new_tweets:
            try:
                await cursor.executemany(
                    "INSERT OR IGNORE INTO tweets (id, token, text, user_name, profile_pic) VALUES (?, ?, ?, ?, ?)",
                    new_tweets,
                )
                await conn.commit()
                logging.info(f"Stored {len(new_tweets)} new tweets for {token}.")
            except Exception as e:
                logging.error(f"Error inserting tweets into DB: {e}")
        else:
            logging.info(f"No new tweets found for {token}.")


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
        await cursor.execute("SELECT id, text, user_name, profile_pic FROM tweets WHERE token = ? ORDER BY id DESC", (token,))
        rows = await cursor.fetchall()

    return [{"id": row[0], "text": row[1], "user_name": row[2], "profile_pic": row[3]} for row in rows]


async def fetch_tweets(token, task_id, db_path):
    """Fetch tweets, store only new ones, and return only new tweets."""
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

        await store_tweets(token, fetched_tweets, db_path)

        logging.info(f"New tweets found: {len(fetched_tweets)} for {token}")
        return fetched_tweets

    except Exception as e:
        logging.error(f"Error fetching tweets for {token} using task {task_id}: {e}")
        return []



def analyze_sentiment(text):
    """Perform sentiment analysis on a tweet using CryptoBERT."""
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding="max_length", max_length=128)
    outputs = model(**inputs).logits
    scores = softmax(outputs.detach().numpy())[0]
    return scores[2]  # WomScore


async def get_sentiment(cashtags, db_path):
    """Fetch tweets, analyze sentiment, and return results."""
    sentiment_results = {}
    task_cycle = itertools.cycle(task_ids)

    async with httpx.AsyncClient() as client:
        tasks = []
        for token in cashtags:
            task_id = next(task_cycle)
            tasks.append(fetch_tweets(token, task_id, db_path))  # Pass db_path

        fetched_tweets = await asyncio.gather(*tasks)

        for token, tweets in zip(cashtags, fetched_tweets):
            if not tweets:
                logging.info(f"No tweets found for {token}.")
                sentiment_results[token] = 0
                continue

            wom_scores = []
            for tweet in tweets:
                raw_text = tweet.get("text", "").strip()
                followers_count = tweet.get("author", {}).get("followers", 0)

                if not raw_text or not is_relevant_tweet(raw_text) or followers_count < 150:
                    continue
                
                sentiment_score = analyze_sentiment(preprocess_tweet(raw_text))
                wom_scores.append(sentiment_score)

            if wom_scores:
                avg_score = sum(wom_scores) / len(wom_scores)
                sentiment_results[token] = round(avg_score * 100, 2)
                logging.info(f"{token} - Average Wom Score: {sentiment_results[token]:.2f}%")
            else:
                sentiment_results[token] = None

    return sentiment_results
