import asyncio
from asyncio.log import logger
import os
import httpx # type: ignore
import logging
import pytz # type: ignore
from dotenv import load_dotenv # type: ignore
from datetime import datetime, timedelta, timezone
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline
from sqlalchemy.dialects.postgresql import insert, delete # type: ignore
import math
import re
import traceback
import random

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

# Global semaphore to limit concurrent requests (10 RPS)
RATE_LIMIT = 10
semaphore = asyncio.Semaphore(RATE_LIMIT)

async def fetch_active_tokens():
    rows = await database.fetch_all(tokens.select().where(tokens.c.is_active == True))
    return [row["token_symbol"] for row in rows]

async def fetch_tweets_from_rapidapi(token_symbol, cursor=None, retries=3):
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    rapidapi_host = os.getenv("RAPIDAPI_HOST")

    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": rapidapi_host
    }

    if not token_symbol:
        logging.error("Token symbol is None or empty.")
        return [], None

    clean_token = token_symbol.strip().replace("$", "")
    query_prefix = "#" if len(clean_token) > 6 else "$"
    query = f"{query_prefix}{clean_token}"

    url = f"https://{rapidapi_host}/search"
    params = {
        "type": "Latest",
        "count": "20",
        "query": query
    }
    if cursor:
        params["cursor"] = cursor

    delay = 1  # initial delay for retries

    for _ in range(retries):
        async with semaphore:
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.get(url, headers=headers, params=params)
                    if response.status_code == 429:
                        logging.warning(f"[429] Rate limited on {token_symbol}, retrying in {delay}s...")
                        await asyncio.sleep(delay)
                        delay *= 2
                        continue

                    response.raise_for_status()
                    data = response.json()

                    instructions = data.get("result", {}).get("timeline", {}).get("instructions", [])
                    entries = []
                    for instr in instructions:
                        if instr.get("type") == "TimelineAddEntries":
                            entries.extend(instr.get("entries", []))

                    tweets = []
                    next_cursor = None

                    for entry in entries:
                        content = entry.get("content", {})

                        if content.get("entryType") == "TimelineTimelineCursor" and content.get("cursorType") == "Bottom":
                            next_cursor = content.get("value")
                            continue

                        item = content.get("itemContent", {})
                        tweet_result = item.get("tweet_results", {}).get("result", {})
                        legacy = tweet_result.get("legacy", {})
                        user = (
                            tweet_result.get("core", {})
                            .get("user_results", {})
                            .get("result", {})
                            .get("legacy", {})
                        )

                        if not legacy or not user:
                            continue

                        tweets.append({
                            "tweet_id": legacy.get("id_str"),
                            "text": legacy.get("full_text"),
                            "created_at": legacy.get("created_at"),
                            "user_info": {
                                "screen_name": user.get("screen_name"),
                                "followers_count": user.get("followers_count", 0),
                                "avatar": user.get("profile_image_url_https", "")
                            },
                            "type": "tweet"
                        })

                    if not tweets:
                        logging.info(f"No tweets found on RapidAPI for {token_symbol}")
                    return tweets, next_cursor

            except httpx.HTTPStatusError as e:
                logging.error(f"RapidAPI HTTP error for {token_symbol}: {e.response.status_code}")
            except httpx.RequestError as e:
                logging.warning(f"RapidAPI request error for {token_symbol}: {e}")
            except Exception as e:
                logging.error(f"RapidAPI unknown error for {token_symbol}: {e}")
        
        await asyncio.sleep(delay)
        delay *= 2  # exponential backoff

    logging.error(f"[FAILED] After {retries} retries for {token_symbol}")
    return [], None

# === Preprocess ===

async def preprocess_tweets(raw_tweets, token_symbol, min_followers=150):
    processed = []
    for tweet in raw_tweets:
        if tweet.get("type") != "tweet":
            continue

        text = tweet.get("text")
        if text is None:
            logger.warning(f"[{token_symbol}] Skipping tweet {tweet.get('tweet_id')} – text is None")
            continue  # skip processing this tweet

        user = tweet.get("user_info", {})
        try:
            dt = datetime.strptime(tweet.get("created_at", ""), "%a %b %d %H:%M:%S %z %Y")
            created_at = dt.astimezone(pytz.utc).isoformat()
        except Exception:
            created_at = None

        data = {
            "tweet_id": tweet.get("tweet_id"),
            "text": text.strip(),
            "user_name": user.get("screen_name", ""),
            "followers_count": user.get("followers_count", 0),
            "profile_pic": user.get("avatar", ""),
            "created_at": created_at,
            "tweet_url": f"https://x.com/{user.get('screen_name')}/status/{tweet.get('tweet_id')}"
        }

        if is_relevant_tweet(data["text"]) and data["followers_count"] >= min_followers:
            processed.append(data)

    if not processed:
        logger.warning(f"[{token_symbol}] No valid tweets after processing.")
        return {}

    return {token_symbol: processed}

# === Sentiment ===

def clean_text(text):
    text = re.sub(r"http\S+", "", text)  # strip URLs
    text = re.sub(r"[^\w\s\$#@]", "", text)  # remove emojis and junk
    return text.strip()

async def analyze_sentiment(text):
    if not text:
        return 1.0

    try:
        predictions = pipe(text)[0]
        print(f"[Sentiment Debug] Text: {text} → Predictions: {predictions}")
        
        # use string labels 
        scores = {pred["label"].lower(): pred["score"] for pred in predictions}

        positive = scores.get("bullish", 0.0)
        neutral = scores.get("neutral", 0.0)

        raw_score = (2.0 * positive) + (0.5 * neutral)  
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

# === One-token workflow ===

async def fetch_and_analyze(token_symbol: str):
    # 1. Sanity check token
    exists = await database.fetch_one(...)
    if not exists:
        return

    # 2. Fetch tweets in last 48h (raw only)
    new_raw = await fetch_last_48h_tweets(token_symbol)

    # 3. Preprocess
    processed_dict = await preprocess_tweets(new_raw, token_symbol)
    tweets = processed_dict.get(token_symbol, [])
    if not tweets:
        return

    # 4. Sentiment
    sentiment_result = await get_sentiment({token_symbol: tweets})
    scored = sentiment_result.get(token_symbol, {}).get("tweets", [])

    # 5. Deduplicate
    existing_ids = set(
        t["tweet_id"] for t in await fetch_stored_tweets(token_symbol)
    )
    new_tweets = [t for t in scored if t["tweet_id"] not in existing_ids]

    # 6. Store only new tweets
    await store_tweets(token_symbol, new_tweets)

    # 7. Prune old tweets (>48h)
    delete_stmt = delete(tweets).where(
            (tweets.c.token_symbol == token_symbol.lower()) &
            (tweets.c.created_at < datetime.now(timezone.utc) - timedelta(hours=48))
        )
    await database.execute(delete_stmt)

    # 8. Fetch remaining tweets for this token
    stored = await fetch_stored_tweets(token_symbol)
    fresh = [t for t in stored if t["created_at"] and t["wom_score"] is not None]

    # 9. Final WOM score
    final_score = compute_final_wom_score(fresh)
    await update_token_table(token_symbol, final_score, len(fresh))

    logging.info(f"[{token_symbol}] WOM={final_score} from {len(fresh)} tweets.")

def compute_final_wom_score(tweets):
    """Compute final WOM score using time decay and scaling."""
    if not tweets:
        return 1.0

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

        wom_score = tweet["wom_score"] or 0
        age_hours = (now - created_at).total_seconds() / 3600
        weight = math.exp(-age_hours / decay_constant)

        weighted_sum += wom_score * weight
        total_weight += weight

    if total_weight == 0:
        return 0.0

    average_score = weighted_sum / total_weight if total_weight else 1.0

    # Bayesian smoothing
    global_avg = 50.0    # mid-range confidence score (adjust based on observed averages)
    confidence = 15      # number of tweets needed to fully "trust" the score

    final_score = ((confidence * global_avg) + (len(tweets) * average_score)) / (confidence + len(tweets))
    return round(final_score, 2)


# === Stateless fetching ===

TWEET_TIME_WINDOW_HOURS = 48
MAX_FETCH_PAGES = 5  # prevent infinite loops

def try_parse_twitter_time(ts):
    try:
        return datetime.strptime(ts, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc)
    except Exception:
        logging.warning(f"[Parser] Invalid tweet time format: {ts}")
        return None

async def fetch_last_48h_tweets(token_symbol: str):
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=TWEET_TIME_WINDOW_HOURS)

    all_tweets = []
    seen_ids = set()
    cursor = None
    pages = 0

    while pages < MAX_FETCH_PAGES:
        raw_batch, next_cursor = await fetch_tweets_from_rapidapi(token_symbol, cursor=cursor)
        if not raw_batch:
            break

        # Filter by timestamp
        filtered = []
        for tweet in raw_batch:
            created_at = try_parse_twitter_time(tweet.get("created_at"))
            if not created_at:
                continue

            if created_at >= start_time and tweet["tweet_id"] not in seen_ids:
                tweet["created_at"] = created_at.isoformat()
                filtered.append(tweet)
                seen_ids.add(tweet["tweet_id"])

        if not filtered:
            break

        all_tweets.extend(filtered)

        # Stop if the oldest tweet is outside the 48h window
        oldest = min(try_parse_twitter_time(t["created_at"]) for t in filtered if t.get("created_at"))
        if oldest < start_time:
            break

        cursor = next_cursor
        if not cursor:
            break
        pages += 1

    return all_tweets

# === Run for all active tokens ===

async def run_tweet_pipeline():
    logging.info("Running tweet pipeline...")

    active_tokens = await fetch_active_tokens()
    if not active_tokens:
        logging.warning("No active tokens found.")
        return

    results = await asyncio.gather(
        *(fetch_and_analyze(token) for token in active_tokens),
        return_exceptions=True
    )

    for token, result in zip(active_tokens, results):
        if isinstance(result, Exception):
            logging.error(f"[tweet_pipeline] Token: {token} → {repr(result)}")
        else:
            logging.info(f"[tweet_pipeline] Token: {token} processed.")
