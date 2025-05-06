import asyncio
import os
import httpx
import logging
import pytz
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
import math
import re

from db import database
from models import tokens, tweets
from utils import is_relevant_tweet

# === Setup ===
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)
load_dotenv()

# === CryptoBERT Sentiment Model ===
MODEL_NAME = "ElKulako/cryptobert"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
pipe = TextClassificationPipeline(model=model, tokenizer=tokenizer, top_k=None)

# === Tweet Fetching ===

async def fetch_active_tokens():
    rows = await database.fetch_all(tokens.select().where(tokens.c.is_active == True))
    return [row["token_symbol"] for row in rows]

async def fetch_tweets_from_rapidapi(token_symbol):
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    rapidapi_host = os.getenv("RAPIDAPI_HOST")

    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": rapidapi_host
    }

    if not token_symbol:
        logging.error("Token symbol is None or empty.")
        return []

    clean_token = token_symbol.strip().replace("$", "")
    query_prefix = "#" if len(clean_token) > 6 else "$"
    query = f"{query_prefix}{clean_token}"

    url = f"https://{rapidapi_host}/search.php"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, params={"query": query, "search_type": "Latest"})
            response.raise_for_status()
            return response.json().get("timeline", [])
        except Exception as e:
            logging.error(f"RapidAPI error for {token_symbol}: {e}")
            return []

# === Preprocess ===

async def preprocess_tweets(raw_tweets, token_symbol, min_followers=150):
    processed = []
    for tweet in raw_tweets:
        if tweet.get("type") != "tweet":
            continue

        user = tweet.get("user_info", {})
        try:
            dt = datetime.strptime(tweet.get("created_at", ""), "%a %b %d %H:%M:%S %z %Y")
            created_at = dt.astimezone(pytz.utc).isoformat()
        except Exception:
            created_at = None

        data = {
            "tweet_id": tweet.get("tweet_id"),
            "text": tweet.get("text", "").strip(),
            "user_name": user.get("screen_name", ""),
            "followers_count": user.get("followers_count", 0),
            "profile_pic": user.get("avatar", ""),
            "created_at": created_at,
            "tweet_url": f"https://x.com/{user.get('screen_name')}/status/{tweet.get('tweet_id')}"
        }

        if is_relevant_tweet(data["text"]) and data["followers_count"] >= min_followers:
            processed.append(data)

    return {token_symbol: processed}

# === Sentiment ===

def clean_text(text):
    text = re.sub(r"http\S+", "", text)  # strip URLs
    text = re.sub(r"[^\w\s\$#@]", "", text)  # remove emojis and junk
    return text.strip()

async def analyze_sentiment(text):
    if not text:
        return 1.0  # fallback score if empty

    try:
        predictions = pipe(text)[0]  # [{label: ..., score: ...}, ...]
        scores = {pred["label"]: pred["score"] for pred in predictions}

        positive = scores.get("LABEL_2", 0.0)
        neutral = scores.get("LABEL_1", 0.0)

        # WOM Score logic: prioritize positive, but neutral adds something
        raw_score = (2.0 * positive) + (0.5 * neutral)  # Max possible: 2.5

        # Normalize to a 0–88 scale (cap at 2.5 to avoid spikes)
        normalized = min(raw_score, 2.5) / 2.5 * 88
        return round(normalized, 2)

    except Exception as e:
        logging.error(f"Sentiment error: {e}")
        return 1.0


async def get_sentiment(tweets_by_token):
    sentiment_results = {}

    for token, tweets in tweets_by_token.items():
        if not tweets:
            continue  # No tweets for this token, skip

        # Analyze all tweets' text in parallel
        texts = [clean_text(t["text"]) for t in tweets]
        scores = await asyncio.gather(*(analyze_sentiment(text) for text in texts))

        # Attach the sentiment score to each tweet
        for tweet, score in zip(tweets, scores):
            tweet["wom_score"] = score

        # Compute average WOM score for this batch (simple, no weighting here)
        avg = round(sum(scores) / len(scores) * 50, 2)

        sentiment_results[token] = {
            "wom_score": avg,
            "tweet_count": len(tweets),
            "tweets": tweets
        }

    return sentiment_results

# === Store & Update ===

async def store_tweets(token: str, tweets_list: list[dict]) -> None:
    """
    Idempotently bulk-insert tweets for *token*.
    •   Skips empty/invalid rows up-front.
    •   Normalises the token only once.
    •   Uses a single list-comprehension guarded by a helper
        so we don’t do try/except work twice.
    """

    def _transform(tweet: dict):
        """Return a row-dict or None if parsing fails / field missing."""
        try:
            # ISO-8601 strings coming back from preprocess_tweets()
            dt = datetime.fromisoformat(tweet["created_at"])
            if dt.tzinfo is None:               # tolerate naïve datetimes
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

        return {
            "tweet_id":        tweet["tweet_id"],
            "token_symbol":    token_lc,              # use cached lowercase
            "text":            tweet["text"],
            "user_name":       tweet["user_name"],
            "followers_count": tweet["followers_count"],
            "profile_pic":     tweet["profile_pic"],
            "created_at":      dt,
            "wom_score":       tweet["wom_score"],
            "tweet_url":       tweet["tweet_url"],
        }

    if not tweets_list:                 # nothing to do
        return

    token_lc = token.lower()

    # build rows, silently dropping any that fail _transform()
    rows = [_transform(t) for t in tweets_list]
    rows = [r for r in rows if r is not None]

    if not rows:
        return  # every row failed validation

    stmt = insert(tweets).on_conflict_do_nothing(index_elements=["tweet_id"])
    await database.execute_many(stmt, rows)


async def update_token_table(token, wom_score, count):
    stmt = tokens.update().where(
        tokens.c.token_symbol == token.lower()
    ).values(
        wom_score=wom_score,
        tweet_count=count
    )
    await database.execute(stmt)

# === Query ===

async def fetch_stored_tweets(token):
    query = tweets.select().where(tweets.c.token_symbol == token.lower())
    return await database.fetch_all(query)

async def fetch_tweet_volume_last_6h(token):
    now = datetime.now(timezone.utc)
    six_hours_ago = now - timedelta(hours=6)
    query = tweets.select().with_only_columns(tweets.c.created_at).where(
        (tweets.c.token_symbol == token.lower()) &
        (tweets.c.created_at >= six_hours_ago)
    )
    rows = await database.fetch_all(query)
    volume = {f"Hour -{i}": 0 for i in range(6, 0, -1)}
    for row in rows:
        created = row["created_at"]
        if isinstance(created, str):
            created = datetime.fromisoformat(created).replace(tzinfo=timezone.utc)

        hours_ago = int((now - created).total_seconds() // 3600)
        if 0 <= hours_ago < 6:
            volume[f"Hour -{hours_ago + 1}"] += 1
    return volume

async def fetch_tweet_volume_buckets(token):
    now = datetime.now(timezone.utc)
    buckets = {
        "1h": now - timedelta(hours=1),
        "6h": now - timedelta(hours=6),
        "12h": now - timedelta(hours=12),
        "24h": now - timedelta(hours=24),
        "48h": now - timedelta(hours=48),
    }

    rows = await database.fetch_all(
        select(tweets.c.created_at).where(tweets.c.token_symbol == token.lower())
    )

    volume = {key: 0 for key in buckets}
    for row in rows:
        created = row["created_at"]
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        for k, dt in buckets.items():
            if created >= dt:
                volume[k] += 1

    return volume

# === One-token workflow ===

async def fetch_and_analyze(token_symbol, store=True):
    # 1. Check if token exists in the DB
    exists_query = select(tokens.c.token_symbol).where(tokens.c.token_symbol == token_symbol.lower())
    exists = await database.fetch_one(exists_query)

    if not exists:
        logging.warning(f"Skipping analysis: Token {token_symbol} not found in tokens table.")
        return

    # 2. Fetch raw tweets
    raw = await fetch_tweets_from_rapidapi(token_symbol)
    if not raw:
        logging.info(f"No new tweets found for {token_symbol}. Skipping update.")
        return

    # 3. Preprocess tweets
    processed_dict = await preprocess_tweets(raw, token_symbol)
    processed = processed_dict.get(token_symbol, [])

    if not processed:
        logging.info(f"All tweets filtered out for {token_symbol}. Skipping update.")
        return

    # 4. Sentiment analysis
    sentiment_dict = await get_sentiment({token_symbol: processed})
    sentiment = sentiment_dict.get(token_symbol, {})

    if store and sentiment.get("tweets"):
        # 5. Store new tweets
        await store_tweets(token_symbol, sentiment["tweets"])

        # 6. Fetch ALL stored tweets to calculate final WOM score
        all_stored = await fetch_stored_tweets(token_symbol)
        filtered = [t for t in all_stored if t["wom_score"] is not None and t["created_at"] is not None]

        if filtered:
            final_score = compute_final_wom_score(filtered)
            await update_token_table(token_symbol, final_score, len(filtered))
            logging.info(f"Updated {token_symbol}: WOM Score={final_score}, Tweets={len(filtered)}")

def compute_final_wom_score(tweets):
    """Compute final WOM score using time decay and scaling."""
    if not tweets:
        return 0.0

    now = datetime.now(timezone.utc)
    decay_constant = 12  # Decay in hours
    weighted_sum = 0.0
    total_weight = 0.0

    for tweet in tweets:
        created_at = tweet["created_at"]
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        wom_score = tweet.get("wom_score", 0)
        age_hours = (now - created_at).total_seconds() / 3600
        weight = math.exp(-age_hours / decay_constant)

        weighted_sum += wom_score * weight
        total_weight += weight

    if total_weight == 0:
        return 0.0

    average_score = weighted_sum / total_weight  
    final_score = round((average_score / 2) * 88, 2)  

    return final_score

# === Run for all active tokens ===

async def run_tweet_pipeline():
    active_tokens = await fetch_active_tokens()
    await asyncio.gather(*(fetch_and_analyze(token) for token in active_tokens))
