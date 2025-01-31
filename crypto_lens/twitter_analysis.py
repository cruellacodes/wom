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
actor_id = os.getenv("TOKEN_ACTOR")
cookies = os.getenv("COOKIES")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

client = ApifyClient(api_token)

# Load sentiment analysis model
MODEL_NAME = "ElKulako/cryptobert"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)


def fetch_tweets(token):
    """Fetch latest tweets using the Apify actor"""

    # Determine the search term format
    search_value = token.lower().replace("$", "")
    search_term = f"${search_value}" if len(token) <= 6 else f"#{search_value}"

    # Prepare the Actor input
    run_input = {
        "searchTerms": [search_term],  # Dynamically use $TOKEN or #TOKEN
        "sortBy": "Latest",
        "maxItems": 50,
        "minRetweets": 0,
        "minLikes": 0,
        "minReplies": 0,
        "tweetLanguage": "en",  # Fetch only English tweets
    }

    try:
        print(f"🚀 Fetching tweets for {token} using '{search_term}'...")
        run = client.actor(actor_id).call(run_input=run_input)
        tweets = list(client.dataset(run["defaultDatasetId"]).iterate_items())

        print(f"✅ Retrieved {len(tweets)} tweets for {token}.")
        return tweets

    except Exception as e:
        print(f"❌ Error fetching tweets for {token}: {e}")
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
