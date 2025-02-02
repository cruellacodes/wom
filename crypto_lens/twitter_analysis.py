import os
import requests
import pandas as pd
from apify_client import ApifyClient
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
from dotenv import load_dotenv
from utils import preprocess_tweet, is_relevant_tweet
import logging
import concurrent.futures
import itertools
import statistics

# Configure logging with a professional format
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)

# Load environment variables
load_dotenv()

# Initialize Apify Client
api_token = os.getenv("APIFY_API_TOKEN")
task_ids_str = os.getenv("WORKER_IDS") 
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")
if not task_ids_str:
    raise ValueError("TOKEN_ACTORS not set in environment variables!")

task_ids = [tid.strip() for tid in task_ids_str.split(",") if tid.strip()]
cookies = os.getenv("COOKIES")

client = ApifyClient(api_token)

# Load sentiment analysis model (Cryptobert)
MODEL_NAME = "ElKulako/cryptobert"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)

def update_task_input(task_id, new_input):
    """
    Update the task input using a direct HTTP PUT request.
    
    Args:
        task_id (str): The Apify task ID or 'username~taskName'.
        new_input (dict): The new input to set for the task.
        
    Returns:
        dict: The updated task input as returned by the Apify API.
    """
    url = f"https://api.apify.com/v2/actor-tasks/{task_id}/input"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    response = requests.put(url, headers=headers, json=new_input)
    response.raise_for_status()
    return response.json()


def fetch_tweets(token, task_id):
    """
    Update the task input for a specific token, run the task, and return tweets.
    
    Args:
        token (str): The token symbol (e.g., "$BINANCE" or "$ETH").
        task_id (str): The Apify task ID to use.
    
    Returns:
        List[dict]: List of tweets fetched by the task for a specific token
    """
    # Determine the search term format.
    search_value = token.lower().replace("$", "")
    search_term = f"${search_value}" if len(token) <= 6 else f"#{search_value}"

    # Prepare the task input
    new_input = {
        "searchTerms": [search_term],  # Use $TOKEN or #TOKEN dynamically
        "sortBy": "Latest",
        "maxItems": 50,
        "minRetweets": 0,
        "minLikes": 0,
        "minReplies": 0,
        "tweetLanguage": "en",  # Fetch only English tweets
    }

    try:
        update_task_input(task_id, new_input)
        run = client.task(task_id).call()
        tweets = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        logging.info(f"Retrieved {len(tweets)} tweets for {token} using task {task_id}.")
        return tweets
    
    except Exception as e:
        logging.error(f"Error fetching tweets for {token} using task {task_id}: {e}")
        return []


def analyze_sentiment(text):
    """Perform sentiment analysis on a tweet."""
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
    outputs = model(**inputs).logits
    scores = softmax(outputs.detach().numpy())[0]
    return scores[2]  # Bullishness score


def analyze_cashtags(cashtags):
    """
    Fetch tweets, analyze sentiment, and return results as a Pandas DataFrame.
    Uses multiple tasks concurrently to speed up tweet fetching.
    """
    data = []
    # Create a cycle iterator for the task IDs so that each token is assigned a task in round-robin fashion
    task_cycle = itertools.cycle(task_ids)
    
    # Use a ThreadPoolExecutor to fetch tweets in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(task_ids)) as executor:
        # Submit a fetch_tweets task for each token, each with a different task ID
        future_to_token = {
            executor.submit(fetch_tweets, token, next(task_cycle)): token
            for token in cashtags
        }
        
        for future in concurrent.futures.as_completed(future_to_token):
            token = future_to_token[future]
            tweets = future.result()
            if not tweets:
                logging.info(f"No tweets found for {token}.")
                continue

            logging.info(f"Analyzing {len(tweets)} tweets for {token}...")
            bullishness_scores = []

            for tweet in tweets:
                raw_text = tweet.get("text", "").strip()
                created_at = tweet.get("createdAt")
                followers_count = tweet.get("author", {}).get("followers", 0)

                # Filter out irrelevant tweets or those from users with fewer than 150 followers
                if not is_relevant_tweet(raw_text) or followers_count < 150:
                    continue

                sentiment_score = analyze_sentiment(preprocess_tweet(raw_text))
                bullishness_scores.append(sentiment_score)

                data.append({
                    "Cashtag": token,
                    "Bullishness": sentiment_score,
                    "Created_At": created_at,
                    "Tweet_Text": raw_text,
                })

            if bullishness_scores:
                # Calculate the median bullishness score and convert to a percentage
                median_score = statistics.median(bullishness_scores) * 100
                logging.info(f"{token} - Median Bullishness Score: {median_score:.2f}%")
    
    return pd.DataFrame(data)
