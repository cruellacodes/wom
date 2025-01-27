import os
import sqlite3
import pandas as pd
from apify_client import ApifyClient
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
from utils import preprocess_tweet, is_relevant_tweet
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database setup
DB_FILE = "tweets.db"

# Load FinBERT model and tokenizer
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

# Initialize ApifyClient
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")
client = ApifyClient(api_token)


# Database Functions
def init_db():
    """Initialize the database and create required tables."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Create `tokens` table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_symbol VARCHAR(10) UNIQUE NOT NULL,
        latest_tweet_id VARCHAR(20)
    )
    """)

    # Create `tweets` table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tweets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tweet_id VARCHAR(20) UNIQUE NOT NULL,
        token_symbol VARCHAR(10) NOT NULL,
        text TEXT,
        created_at TIMESTAMP,
        followers_count INTEGER,
        sentiment_score FLOAT,
        FOREIGN KEY (token_symbol) REFERENCES tokens (token_symbol)
    )
    """)

    conn.commit()
    conn.close()


def update_token(conn, token_symbol, latest_tweet_id=None):
    """Insert or update token metadata in the database."""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO tokens (token_symbol, latest_tweet_id)
        VALUES (?, ?)
        ON CONFLICT(token_symbol) DO UPDATE SET
            latest_tweet_id=excluded.latest_tweet_id
    """, (token_symbol, latest_tweet_id))
    conn.commit()


def insert_tweet(conn, tweet_id, token_symbol, text, created_at, followers_count, sentiment_score):
    """Insert tweet data into the database."""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO tweets (tweet_id, token_symbol, text, created_at, followers_count, sentiment_score)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (tweet_id, token_symbol, text, created_at, followers_count, sentiment_score))
    conn.commit()


def get_latest_tweet_id(conn, token_symbol):
    """Fetch the latest tweet ID for a token."""
    cursor = conn.cursor()
    cursor.execute("SELECT latest_tweet_id FROM tokens WHERE token_symbol = ?", (token_symbol,))
    result = cursor.fetchone()
    return result[0] if result else None


# Sentiment Analysis Function
def calculate_bullishness(tweet_text):
    """Analyze sentiment and calculate bullishness percentage using FinBERT."""
    inputs = tokenizer(tweet_text, return_tensors="pt", truncation=True, padding=True)
    outputs = model(**inputs)
    probs = softmax(outputs.logits.detach().numpy()[0])

    # Probabilities for bullish, neutral, and bearish
    bullish_prob = round(probs[0] * 100, 2)
    bearish_prob = round(probs[2] * 100, 2)
    bullishness_score = round(bullish_prob / (bullish_prob + bearish_prob), 2)
    return bullishness_score


def analyze_cashtags(cashtags):
    """
    Analyze sentiment for a list of cashtags, efficiently fetching tweets based on database state.
    """
    # Initialize the database connection
    conn = sqlite3.connect(DB_FILE)

    data = []

    for cashtag in cashtags:
        token_symbol = cashtag

        # Get the latest tweet ID from the database
        latest_tweet_id = get_latest_tweet_id(conn, token_symbol)

        if latest_tweet_id is None:
            # Case 1: No tweets in the database for this token (first run)
            print(f"No tweets found for {cashtag}. Fetching up to 100 tweets...")
            run_input = {
                "searchTerms": [cashtag[1:]],
                "maxItems": 100,
                "sort": "Latest",
                "tweetLanguage": "en",
            }
            try:
                run = client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input)
                fetched_tweets = list(client.dataset(run["defaultDatasetId"]).iterate_items())
            except Exception as e:
                print(f"Error fetching tweets for {cashtag}: {e}")
                continue
        else:
            # Case 2: Tweets already exist; fetch only 1 tweet to check for updates
            print(f"Checking for new tweets for {cashtag}...")
            run_input = {
                "searchTerms": [cashtag[1:]],
                "maxItems": 1,
                "sort": "Latest",
                "tweetLanguage": "en",
            }
            try:
                run = client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input)
                fetched_tweets = list(client.dataset(run["defaultDatasetId"]).iterate_items())
            except Exception as e:
                print(f"Error fetching tweets for {cashtag}: {e}")
                continue

            # If no new tweets, skip this token
            if not fetched_tweets:
                print(f"No new tweets for {cashtag}. Skipping.")
                continue

            # Compare the latest fetched tweet ID with the stored ID
            fetched_latest_tweet_id = fetched_tweets[0].get("id")
            if fetched_latest_tweet_id == latest_tweet_id:
                print(f"No new tweets for {cashtag}. Skipping.")
                continue

            # Fetch new tweets incrementally
            print(f"Fetching new tweets for {cashtag}...")
            run_input["maxItems"] = 10  # Fetch 10 tweets per batch
            fetched_tweets = []
            total_tweets_fetched = 0  # Counter for the total number of tweets fetched
            max_iterations = 5  # Limit to 5 iterations
            iteration_count = 0

            while iteration_count < max_iterations and total_tweets_fetched < 50:
                try:
                    run = client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input)
                    batch_tweets = list(client.dataset(run["defaultDatasetId"]).iterate_items())

                    # Stop fetching if no new tweets are found
                    if not batch_tweets or batch_tweets[-1].get("id") <= latest_tweet_id:
                        break

                    # Add only newer tweets to the fetched list
                    fetched_tweets += [
                        tweet for tweet in batch_tweets
                        if tweet.get("id") > latest_tweet_id
                    ]
                    total_tweets_fetched += len(batch_tweets)
                    iteration_count += 1

                    print(f"Iteration {iteration_count}: Fetched {len(batch_tweets)} tweets. Total: {total_tweets_fetched}")

                except Exception as e:
                    print(f"Error during incremental fetching for {cashtag}: {e}")
                    break

        # Process fetched tweets
        if fetched_tweets:
            print(f"Processing {len(fetched_tweets)} tweets for {cashtag}...")
            bullishness_scores = []
            new_tweets = []

            for item in fetched_tweets:
                tweet_id = item.get("id")
                raw_tweet = item.get("text", "").strip()
                tweet_text = preprocess_tweet(raw_tweet)
                created_at = item.get("createdAt")
                author_info = item.get("author", {})
                followers_count = author_info.get("followers", 0)

                if not is_relevant_tweet(tweet_text) or followers_count < 150:
                    continue

                # Calculate sentiment
                score = calculate_bullishness(tweet_text)
                bullishness_scores.append(score)

                # Store tweet in database
                insert_tweet(conn, tweet_id, token_symbol, raw_tweet, created_at, followers_count, score)

                # Collect data for analysis
                new_tweets.append(tweet_id)
                data.append(
                    {
                        "Cashtag": cashtag,
                        "Bullishness": score,
                        "created_at": created_at,
                        "Tweet_Text": raw_tweet,
                    }
                )

            # Update the latest tweet ID
            if new_tweets:
                update_token(conn, token_symbol, max(new_tweets))

    conn.close()
    return pd.DataFrame(data)
