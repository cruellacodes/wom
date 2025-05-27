import asyncio
from asyncio.log import logger
import os
import httpx # type: ignore
import logging
from dotenv import load_dotenv # type: ignore
from datetime import datetime, timedelta, timezone
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline
from sqlalchemy.dialects.postgresql import insert # type: ignore
from sqlalchemy import delete  # type: ignore
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
                logging.warning(f"RapidAPI request error for {token_symbol}: {e.__class__.__name__} → {str(e)}")
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
            created_at = normalize_created_at(tweet["created_at"])
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

    if not tweets_list:
        return

    token_lc = token.lower()

    def _transform(tweet: dict):
        try:
            dt = datetime.fromisoformat(tweet["created_at"])
        except Exception:
            logging.warning(f"[Storage] Invalid ISO timestamp: {tweet['created_at']}")
            return None

        return {
            "tweet_id":        tweet["tweet_id"],
            "token_symbol":    token_lc,
            "text":            tweet["text"],
            "user_name":       tweet["user_name"],
            "followers_count": tweet["followers_count"],
            "profile_pic":     tweet["profile_pic"],
            "created_at":      dt,
            "wom_score":       tweet["wom_score"],
            "tweet_url":       tweet["tweet_url"],
        }

    rows = [_transform(t) for t in tweets_list]
    rows = [r for r in rows if r is not None]

    if not rows:
        return

    query = """
        INSERT INTO tweets (
            tweet_id, token_symbol, text, user_name, followers_count, profile_pic, created_at, wom_score, tweet_url
        )
        VALUES (
            :tweet_id, :token_symbol, :text, :user_name, :followers_count, :profile_pic, :created_at, :wom_score, :tweet_url
        )
        ON CONFLICT (tweet_id) DO NOTHING
    """

    await database.execute_many(query=query, values=rows)

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

async def fetch_and_store_tweets(token_symbol: str):
    new_raw = await fetch_last_48h_tweets(token_symbol)
    if not new_raw:
        return

    processed_dict = await preprocess_tweets(new_raw, token_symbol)
    tweets = processed_dict.get(token_symbol, [])
    if not tweets:
        return

    sentiment_result = await get_sentiment({token_symbol: tweets})
    scored = sentiment_result.get(token_symbol, {}).get("tweets", [])
    if not scored:
        return

    existing_ids = set(
        t["tweet_id"] for t in await fetch_stored_tweets(token_symbol)
    )
    new_tweets = [t for t in scored if t["tweet_id"] not in existing_ids]
    if not new_tweets:
        return

    await store_tweets(token_symbol, new_tweets)

async def recalculate_token_scores():
    active = await fetch_active_tokens()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=48)

    for token in active:
        tweets = await fetch_stored_tweets(token)
        recent = [
            t for t in tweets
            if t["created_at"] and t["wom_score"] is not None
            and try_parse_twitter_time(t["created_at"]) >= cutoff
        ]

        if not recent:
            logging.info(f"[{token}] No recent tweets. Skipping WOM score update.")
            continue

        final_score = compute_final_wom_score(recent)
        await update_token_table(token, final_score, len(recent))
        logging.info(f"[{token}] WOM={final_score} from {len(recent)} tweets.")

def compute_final_wom_score(tweets: list[dict]) -> float:
    """
    Compute the final WOM score for a list of tweets using exponential decay
    and Bayesian smoothing. Assumes all tweets have:
    - 'created_at' as timezone-aware datetime
    - 'wom_score' as a float
    """
    if not tweets:
        return 1.0

    now = datetime.now(timezone.utc)
    decay_hours = 12
    weight_sum = 0.0
    score_sum = 0.0

    for tweet in tweets:
        age_hours = (now - tweet["created_at"]).total_seconds() / 3600
        weight = math.exp(-age_hours / decay_hours)
        score_sum += tweet["wom_score"] * weight
        weight_sum += weight

    avg_score = score_sum / weight_sum if weight_sum else 50.0

    # Bayesian smoothing
    prior = 50.0
    confidence = 15
    smoothed = ((confidence * prior) + (len(tweets) * avg_score)) / (confidence + len(tweets))

    # Volume weighting
    def volume_boost(count, mid=5):
        return 1 / (1 + math.exp(-0.5 * (count - mid)))

    volume_weight = volume_boost(len(tweets))
    final_score = smoothed * volume_weight

    return round(min(final_score, 100.0), 2)

# === Stateless fetching ===

TWEET_TIME_WINDOW_HOURS = 48
MAX_FETCH_PAGES = 10  # prevent infinite loops

def normalize_created_at(raw_time: str) -> str | None:
    if not raw_time:
        return None
    try:
        # Case 1: Already in ISO format
        return datetime.fromisoformat(raw_time).astimezone(timezone.utc).isoformat()
    except ValueError:
        pass
    try:
        # Case 2: Twitter classic format
        dt = datetime.strptime(raw_time, "%a %b %d %H:%M:%S %z %Y")
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        logging.warning(f"[Parser] Invalid tweet time format in preprocess: {raw_time}")
        return None



def try_parse_twitter_time(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        # Case 1: ISO-8601 (already processed earlier)
        return datetime.fromisoformat(ts).astimezone(timezone.utc)
    except ValueError:
        pass
    try:
        # Case 2: Twitter classic format
        return datetime.strptime(ts, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc)
    except Exception:
        logging.warning(f"[Parser] Invalid tweet time format: {ts}")
        return None


async def fetch_last_48h_tweets(token_symbol: str):
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=48)

    all_tweets = []
    seen_ids = set()
    cursor = None
    pages = 0
    MAX_PAGES = 10
    MAX_TWEETS = 200

    while pages < MAX_PAGES:
        raw_batch, next_cursor = await fetch_tweets_from_rapidapi(token_symbol, cursor=cursor)
        if not raw_batch:
            break

        filtered_batch = []
        stop_pagination = False

        for tweet in raw_batch:
            created_at = try_parse_twitter_time(tweet.get("created_at"))
            if not created_at:
                continue

            if created_at < start_time:
                stop_pagination = True
                continue

            if tweet["tweet_id"] in seen_ids:
                continue

            tweet["created_at"] = created_at.isoformat()
            filtered_batch.append(tweet)
            seen_ids.add(tweet["tweet_id"])

        all_tweets.extend(filtered_batch)
        logging.info(f"[{token_symbol}] Page {pages+1}: {len(filtered_batch)} valid tweets")

        if stop_pagination or not next_cursor or len(all_tweets) >= MAX_TWEETS:
            break

        cursor = next_cursor
        pages += 1

    logging.info(f"[{token_symbol}] Total tweets fetched: {len(all_tweets)}")
    return all_tweets

# === Run for all active tokens ===

async def run_tweet_pipeline():
    logging.info("Running tweet pipeline...")

    active_tokens = await fetch_active_tokens()
    if not active_tokens:
        logging.warning("No active tokens found.")
        return

    results = await asyncio.gather(
        *(fetch_and_store_tweets(token) for token in active_tokens),
        return_exceptions=True
    )

    for token, result in zip(active_tokens, results):
        if isinstance(result, Exception):
            logging.error(f"[tweet_pipeline] Token: {token} → {repr(result)}")
        else:
            logging.info(f"[tweet_pipeline] Token: {token} processed.")

async def run_score_pipeline():
    logging.info("Recalculating final WOM scores...")

    active_tokens = await fetch_active_tokens()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=48)

    for token in active_tokens:
        try:
            stored = await fetch_stored_tweets(token)
            valid = [
                t for t in stored
                if t["created_at"] and t["wom_score"] is not None and
                t["created_at"] >= cutoff
            ]


            if not valid:
                logging.info(f"[{token}] No valid recent tweets, skipping score update.")
                continue

            final_score = compute_final_wom_score(valid)
            await update_token_table(token, final_score, len(valid))
            logging.info(f"[{token}] Final WOM={final_score} from {len(valid)} tweets.")

        except Exception as e:
            logging.error(f"[score_pipeline] Token: {token} → {repr(e)}")

    await prune_old_tweets()
    logging.info("Old tweets >48h pruned.")

async def prune_old_tweets():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
    stmt = delete(tweets).where(tweets.c.created_at < cutoff)
    await database.execute(stmt)

async def handle_on_demand_search(token_symbol: str) -> dict:
    raw_tweets = await fetch_last_48h_tweets(token_symbol)
    processed = await preprocess_tweets(raw_tweets, token_symbol)
    tweets = processed.get(token_symbol, [])

    if not tweets:
        return {"token_symbol": token_symbol, "tweets": [], "wom_score": 0.0}

    sentiment_result = await get_sentiment({token_symbol: tweets})
    scored = sentiment_result.get(token_symbol, {}).get("tweets", [])

    for tweet in scored:
        if isinstance(tweet["created_at"], str):
            tweet["created_at"] = datetime.fromisoformat(tweet["created_at"])

    score = compute_final_wom_score(scored)
    return {"token_symbol": token_symbol, "tweets": scored, "wom_score": score}