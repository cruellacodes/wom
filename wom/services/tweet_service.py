import asyncio
import os
import httpx
import logging
import pytz
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, delete

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
    if not token_symbol or not isinstance(token_symbol, str):
        logging.error(f"Invalid token_symbol passed: {token_symbol}")
        return []
    
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    rapidapi_host = os.getenv("RAPIDAPI_HOST")

    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": rapidapi_host
    }

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

async def analyze_sentiment(text):
    if not text:
        return 1.0
    try:
        preds = pipe(text)[0]
        return round((1 * preds[1]["score"]) + (2 * preds[2]["score"]), 2)
    except Exception as e:
        logging.error(f"Sentiment error: {e}")
        return 1.0

async def get_sentiment(tweets_by_token):
    sentiment_results = {}

    for token, tweets in tweets_by_token.items():
        if not tweets:
            sentiment_results[token] = {
                "wom_score": 1.0,
                "tweet_count": 0,
                "tweets": []
            }
            continue

        scores = await asyncio.gather(*(analyze_sentiment(t["text"]) for t in tweets))

        for i, score in enumerate(scores):
            tweets[i]["wom_score"] = score

        avg = round((sum(scores) / len(scores)) / 2 * 100, 2) if scores else 1.0

        sentiment_results[token] = {
            "wom_score": avg,
            "tweet_count": len(tweets),
            "tweets": tweets
        }

    return sentiment_results

# === Store & Update ===

async def store_tweets(token, tweets_list):
    if not tweets_list:
        return

    values = []
    for tweet in tweets_list:
        try:
            dt = datetime.fromisoformat(tweet["created_at"]).replace(tzinfo=timezone.utc)
            values.append({
                "tweet_id": tweet["tweet_id"],
                "token_symbol": token.lower(),
                "text": tweet["text"],
                "user_name": tweet["user_name"],
                "followers_count": tweet["followers_count"],
                "profile_pic": tweet["profile_pic"],
                "created_at": dt,
                "wom_score": tweet["wom_score"],
                "tweet_url": tweet["tweet_url"]
            })
        except Exception:
            continue

    stmt = insert(tweets).on_conflict_do_nothing(index_elements=["tweet_id"])
    await database.execute_many(query=stmt, values=values)

async def update_token_table(token, wom_score, count):
    stmt = insert(tokens).values(
        token_symbol=token.lower(),
        wom_score=wom_score,
        tweet_count=count
    ).on_conflict_do_update(
        index_elements=["token_symbol"],
        set_={"wom_score": wom_score, "tweet_count": count}
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
        if 1 <= hours_ago <= 6:
            volume[f"Hour -{hours_ago}"] += 1
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
    raw = await fetch_tweets_from_rapidapi(token_symbol)
    if not raw:
        return {"token": token_symbol, "wom_score": 1.0, "tweet_count": 0, "tweets": []}

    processed_dict = await preprocess_tweets(raw, token_symbol)
    processed = processed_dict.get(token_symbol, [])

    if not processed:
        return {"token": token_symbol, "wom_score": 1.0, "tweet_count": 0, "tweets": []}

    sentiment_dict = await get_sentiment({token_symbol: processed})
    sentiment = sentiment_dict.get(token_symbol, {})

    if store and sentiment.get("tweets"):
        await store_tweets(token_symbol, sentiment["tweets"])

        all_stored = await fetch_stored_tweets(token_symbol)
        all_scores = [t["wom_score"] for t in all_stored if t["wom_score"] is not None]
        final_score = round((sum(all_scores) / len(all_scores)) / 2 * 100, 2) if all_scores else 1.0

        await update_token_table(token_symbol, final_score, len(all_scores))

        sentiment["wom_score"] = final_score
        sentiment["tweet_count"] = len(all_scores)

    return {
        "token": token_symbol,
        "wom_score": sentiment.get("wom_score", 1.0),
        "tweet_count": sentiment.get("tweet_count", 0),
        "tweets": sentiment.get("tweets", [])
    }

# === Run for all active tokens ===

async def run_tweet_pipeline():
    active_tokens = await fetch_active_tokens()
    await asyncio.gather(*(fetch_and_analyze(token) for token in active_tokens))
