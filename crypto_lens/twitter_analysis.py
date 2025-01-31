import os
import pandas as pd
from apify_client import ApifyClient
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
from dotenv import load_dotenv
from utils import preprocess_tweet, is_relevant_tweet

# Load environment variables
load_dotenv()

# Initialize Apify Client
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

client = ApifyClient(api_token)

# Load sentiment analysis model
tokenizer = AutoTokenizer.from_pretrained("CryptoBERT")  # Replace with best model
model = AutoModelForSequenceClassification.from_pretrained("CryptoBERT")

# Define Apify Actor IDs
SHORT_TOKEN_ACTOR = "wHootRXb00ztxCELq"
LONG_TOKEN_ACTOR = "bQ0LeyXn6BO51yFDY"

def get_apify_actor(token):
    """Determine which Apify actor to use based on token length."""
    return SHORT_TOKEN_ACTOR if len(token) <= 6 else LONG_TOKEN_ACTOR

def fetch_tweets(token):
    """Fetch latest tweets using the appropriate Apify actor."""
    actor_id = get_apify_actor(token)

    run_input = {
        "cashtag" if len(token) <= 6 else "hashtag": token.lower().replace("$", ""),
        "sentimentAnalysis": None,
        "sortBy": "Latest",
        "maxItems": 10,
        "minRetweets": 0,
        "minLikes": 0,
        "minReplies": 0,
        "onlyVerifiedUsers": None,
        "onlyBuleVerifiedUsers": None,
    }

    try:
        run = client.actor(actor_id).call(run_input=run_input)
        tweets = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        return tweets
    except Exception as e:
        print(f"Error fetching tweets for {token}: {e}")
        return []

def analyze_sentiment(text):
    """Perform sentiment analysis on a tweet."""
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
    outputs = model(**inputs).logits
    scores = softmax(outputs.detach().numpy())[0]
    return scores[2]  # Bullishness score

def analyze_cashtags(cashtags):
    """Fetch tweets, analyze sentiment, and return results as a Pandas DataFrame."""
    data = []

    for token in cashtags:
        print(f"Fetching tweets for {token}...")
        tweets = fetch_tweets(token)

        if not tweets:
            print(f"No tweets found for {token}.")
            continue

        print(f"Analyzing {len(tweets)} tweets for {token}...")
        bullishness_scores = []

        for tweet in tweets:
            raw_text = tweet.get("text", "").strip()
            created_at = tweet.get("createdAt")
            followers_count = tweet.get("author", {}).get("followers", 0)

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
            avg_score = sum(bullishness_scores) / len(bullishness_scores)
            print(f"✅ {token} - Avg Bullishness Score: {avg_score:.2f}")

    return pd.DataFrame(data)
