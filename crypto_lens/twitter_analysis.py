import itertools
import os
import httpx
import logging
import asyncio
import statistics
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
task_ids_str = os.getenv("WORKER_IDS")  # Using "WORKER_IDS" as in your latest version
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")
if not task_ids_str:
    raise ValueError("WORKER_IDS not set in environment variables!")

# Parse task IDs (comma-separated list)
task_ids = [tid.strip() for tid in task_ids_str.split(",") if tid.strip()]

# Load CryptoBERT model
MODEL_NAME = "ElKulako/cryptobert"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)

async def update_task_input(task_id, new_input):
    """
    Update the task input using a direct HTTP PUT request.
    
    Args:
        task_id (str): The Apify task ID (or 'username~taskName').
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

async def fetch_tweets(token, task_id):
    """
    Update the task input for a token, run the task, and return fetched tweets.
    
    Args:
        token (str): The token symbol (e.g., "$BINANCE" or "$ETH").
        task_id (str): The Apify task ID.
    
    Returns:
        List[dict]: A list of tweets fetched by the task.
    """
    search_value = token.lower().replace("$", "")
    search_term = f"${search_value}" if len(token) <= 6 else f"#{search_value}"

    # Task input payload
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

        # Run task after updating input
        async with httpx.AsyncClient() as client:
            run_response = await client.post(
                f"https://api.apify.com/v2/actor-tasks/{task_id}/runs?token={api_token}"
            )
            run_response.raise_for_status()
            run_id = run_response.json()["data"]["id"]

            # Wait for the run to complete
            while True:
                run_status = await client.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}?token={api_token}"
                )
                run_status.raise_for_status()
                status = run_status.json()["data"]["status"]
                if status == "SUCCEEDED":
                    break
                elif status in ["FAILED", "TIMED_OUT", "ABORTED"]:
                    raise RuntimeError(f"Apify run failed with status: {status}")
                await asyncio.sleep(5)  # Wait 5 seconds before checking again

            # Fetch the dataset items
            dataset_id = run_status.json()["data"]["defaultDatasetId"]
            dataset_response = await client.get(
                f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={api_token}"
            )
            dataset_response.raise_for_status()
            tweets = dataset_response.json()

        logging.info(f"Retrieved {len(tweets)} tweets for {token} using task {task_id}.")
        return tweets
    
    except Exception as e:
        logging.error(f"Error fetching tweets for {token} using task {task_id}: {e}")
        return []

def analyze_sentiment(text):
    """
    Perform sentiment analysis on a tweet using CryptoBERT.
    
    Args:
        text (str): The tweet text.
    
    Returns:
        float: The bullishness score (0 to 1).
    """
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding="max_length", max_length=128)
    outputs = model(**inputs).logits
    scores = softmax(outputs.detach().numpy())[0]
    return scores[2]  # WomScore

async def get_sentiment(cashtags):
    """
    Fetch tweets for each token, analyze sentiment, and return a dictionary mapping tokens to WomScores.
    
    Args:
        cashtags (list): List of token symbols (e.g., ['$BINANCE', '$ETH']).
        
    Returns:
        dict: { 'TOKEN_SYMBOL': WomScore (sentiment percentage) }
    """
    sentiment_results = {}
    task_cycle = itertools.cycle(task_ids)  # Assign tasks in round-robin fashion

    async with httpx.AsyncClient() as client:
        tasks = []
        for token in cashtags:
            task_id = next(task_cycle)
            tasks.append(fetch_tweets(token, task_id))

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

                # Filter out low-quality tweets
                if not raw_text or not is_relevant_tweet(raw_text) or followers_count < 150:
                    continue
                
                sentiment_score = analyze_sentiment(preprocess_tweet(raw_text))
                wom_scores.append(sentiment_score)

            if wom_scores:
                # Compute the average sentiment score
                avg_score = sum(wom_scores) / len(wom_scores)
                sentiment_results[token] = round(avg_score * 100, 2)  # Convert to percentage
                logging.info(f"{token} - Average Wom Score: {sentiment_results[token]:.2f}%")
            else:
                sentiment_results[token] = None
    
    return sentiment_results