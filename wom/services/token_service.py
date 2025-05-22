from datetime import datetime, timedelta, timezone
import os
import httpx
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func
from sqlalchemy import and_, delete, select
from db import database
from models import tokens, tweets
import unicodedata
import logging
import asyncio
import re

# Logging setup
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)

# Load environment variables
load_dotenv()
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

# ────────────────────────────────────────────
# Fetch & Filter Tokens
# ────────────────────────────────────────────

# -- Helper to validate token symbols --
import unicodedata

def is_valid_token_symbol(symbol: str) -> bool:
    symbol_clean = symbol.lstrip("$").strip()

    # Allow symbols that are 2 to 15 characters
    if not (2 <= len(symbol_clean) <= 15):
        return False

    # Allow only alphabetic characters
    if not symbol_clean.isalpha():
        return False

    # Reject emojis or special unicode symbols
    for char in symbol_clean:
        if unicodedata.category(char).startswith("So"):
            return False

    return True

# -- Helper to validate age --
def format_token_age(pair_created_at_ms: int) -> str:
    now = datetime.now(timezone.utc)
    created_at = datetime.fromtimestamp(pair_created_at_ms / 1000, tz=timezone.utc)
    age_seconds = (now - created_at).total_seconds()

    if age_seconds < 3600:
        return "<1h"
    elif age_seconds < 86400:
        hours = int(age_seconds // 3600)
        return f"{hours}h"
    else:
        days = int(age_seconds // 86400)
        return f"{days}d"

# -- Main function to get filtered tokens --
VALID_DEX_IDS = {"meteora", "raydium", "pumpswap"}
MIN_LIQ_USD = 50_000
MIN_MCAP_USD = 200_000
MIN_VOL_USD = 150_000

# -- Helper to validate token symbols --
def is_valid_token_symbol(symbol: str) -> bool:
    # Remove $ only if present
    symbol_clean = symbol.lstrip("$").strip()

    # Length must be 2–15
    if not (2 <= len(symbol_clean) <= 15):
        return False

    # Must be all letters (A–Z, case-insensitive)
    if not symbol_clean.isalpha():
        return False

    # Reject emoji/symbols
    for char in symbol_clean:
        if unicodedata.category(char).startswith("So"):
            return False

    return True

# -- Main function to get filtered tokens --
async def get_filtered_pairs():
    run_input = {
        "limit": 150,
        "pageCount": 2,
        "chain": "solana",
        "allPools": False,
        "timeFrame": "h6",
        "sortOrder": "desc",
        "sortRank": "trendingScoreH6",
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": []
        }
    }

    filtered_tokens = []
    seen_symbols = set()
    timeout = httpx.Timeout(90.0, connect=10.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            response = await client.post(
                f"https://api.apify.com/v2/acts/muhammetakkurtt~dexscreener-scraper/run-sync-get-dataset-items?token={api_token}",
                json=run_input
            )
            response.raise_for_status()
            items = response.json()
            logging.info(f"[INFO] Got {len(items)} tokens from Apify dataset.")
        except Exception as e:
            logging.error(f"[ERROR] Apify actor failed: {e}")
            return []

        for item in items:
            dex = item.get("dexId", "")
            liq = item.get("liquidity", {}).get("usd", 0)
            mcap = item.get("marketCap", 0)
            vol = item.get("volume", {}).get("h24", 0)

            if dex not in VALID_DEX_IDS:
                continue
            if liq < MIN_LIQ_USD or mcap < MIN_MCAP_USD or vol < MIN_VOL_USD:
                continue

            base = item.get("baseToken", {})
            symbol_raw = base.get("symbol", "").strip()
            symbol_with_dollar = f"${symbol_raw.strip().lower()}"
            symbol_clean = symbol_raw.lstrip("$").strip()

            if not is_valid_token_symbol(symbol_clean):
                logging.info(f"[SKIP] Invalid symbol: ${symbol_clean.lower()}")
                continue

            symbol_with_dollar = f"${symbol_clean.lower()}"
            if symbol_with_dollar in seen_symbols:
                continue

            is_believe = "DYN" in item.get("labels", [])
            seen_symbols.add(symbol_with_dollar)
            pair_created_at = item.get("pairCreatedAt")
            age_string = format_token_age(pair_created_at) if pair_created_at else "N/A"

            filtered_tokens.append({
                "token_symbol": symbol_with_dollar,
                "token_name": base.get("name", "Unknown"),
                "image_url": item.get("info", {}).get("imageUrl", None),
                "address": base.get("address", "N/A"),
                "volume_usd": vol,
                "liquidity_usd": liq,
                "market_cap_usd": mcap,
                "priceChange1h": item.get("priceChange", {}).get("h1", 0),
                "is_believe": is_believe,
                "age": age_string,
            })

        logging.info(f"[DONE] {len(filtered_tokens)} tokens passed filters: {[t['token_symbol'] for t in filtered_tokens]}")
        return filtered_tokens

# ────────────────────────────────────────────
# Store Tokens
# ────────────────────────────────────────────
async def store_tokens(tokens_data):
    now = datetime.now(timezone.utc)

    for token in tokens_data:
        token_symbol = token["token_symbol"]

        insert_stmt = insert(tokens).values(
            token_symbol=token_symbol,
            token_name=token.get("token_name"),
            image_url=token.get("image_url"),
            address=token.get("address"),
            age=token.get("age"),
            volume_usd=token.get("volume_usd"),
            liquidity_usd=token.get("liquidity_usd"),
            market_cap_usd=token.get("market_cap_usd"),
            dex_url=f"https://dexscreener.com/solana/{token.get('address')}",
            pricechange1h=token.get("priceChange1h"),
            created_at=now,
            last_seen_at=now,
            is_active=True,
            wom_score=1.0,
            tweet_count=0,
            is_believe=token.get("is_believe", False),
        )

        update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["token_symbol"],
            set_={
                "token_name": insert_stmt.excluded.token_name,
                "address": insert_stmt.excluded.address,
                "image_url": insert_stmt.excluded.image_url,
                "age": insert_stmt.excluded.age,
                "volume_usd": insert_stmt.excluded.volume_usd,
                "liquidity_usd": insert_stmt.excluded.liquidity_usd,
                "market_cap_usd": insert_stmt.excluded.market_cap_usd,
                "dex_url": insert_stmt.excluded.dex_url,
                "pricechange1h": insert_stmt.excluded.pricechange1h,
                "last_seen_at": now,
                "is_active": True,
                "is_believe": insert_stmt.excluded.is_believe,
            }
        )

        await database.execute(update_stmt)

    logging.info(f"Stored/Updated {len(tokens_data)} tokens.")

def parse_age_to_hours(age_str: str) -> float:
    """Parses age strings like '6h', '2d', '<1h' into float hours.
    Returns 0.0 for '<1h' or any unparseable value to skip deactivation."""
    if not age_str:
        return 0.0

    try:
        # Explicitly skip "<1h" and similar
        if age_str.startswith("<"):
            return 0.0

        # Match patterns like '6h', '2d', '1.5d'
        match = re.match(r"(\d+\.?\d*)([hd])", age_str)
        if match:
            value, unit = match.groups()
            value = float(value)
            return value if unit == "h" else value * 24

        logging.warning(f"Unknown age format: {age_str}")
        return 0.0

    except ValueError:
        logging.warning(f"Invalid age value: {age_str}")
        return 0.0


async def deactivate_low_activity_tokens():
    now = datetime.now(timezone.utc)
    twenty_four_hours_ago = now - timedelta(hours=24)

    # Step 1: Count recent tweets per token (last 24h)
    recent_tweet_counts_query = (
        select([
            tweets.c.token_symbol,
            func.count().label("tweet_count_24h")
        ])
        .where(tweets.c.created_at >= twenty_four_hours_ago)
        .group_by(tweets.c.token_symbol)
    )
    recent_counts = await database.fetch_all(recent_tweet_counts_query)
    tweet_count_map = {r["token_symbol"]: r["tweet_count_24h"] for r in recent_counts}

    # Step 2: Get all active tokens with their metadata
    active_tokens_query = select([
        tokens.c.token_symbol,
        tokens.c.address,
        tokens.c.age,  
        tokens.c.tweet_count,
        tokens.c.volume_usd,
        tokens.c.market_cap_usd 
    ]).where(tokens.c.is_active == True)
    tokens_data = await database.fetch_all(active_tokens_query)

    # Step 3: Apply deactivation rules
    tokens_to_deactivate = []

    for token in tokens_data:
        symbol = token["token_symbol"]
        age_str = token["age"]
        tweet_count_total = token["tweet_count"]
        tweet_count_24h = tweet_count_map.get(symbol, 0)
        volume = token["volume_usd"]
        market_cap = token["market_cap_usd"] or 0

        age_hours = parse_age_to_hours(age_str)

        # ────────────────────────────────────────────
        # Deactivation Rules:
        # ────────────────────────────────────────────

        # Rule 1: Created more than 3h ago AND tweet_count < 20
        if age_hours > 3 and tweet_count_total < 20:
            tokens_to_deactivate.append(symbol)
            continue

        # Rule 2: Created more than 24h ago AND volume < $200k
        if age_hours > 24 and volume < 200_000:
            tokens_to_deactivate.append(symbol)
            continue

        # Rule 3: Created more than 23h ago AND < 10 tweets in last 24h
        if age_hours > 23 and tweet_count_24h < 10:
            tokens_to_deactivate.append(symbol)
            continue

        # Rule 4: Market cap less than $40k
        if market_cap < 40_000:
            tokens_to_deactivate.append(symbol)
            continue

    # Step 4: Update DB
    if tokens_to_deactivate:
        update_query = (
            tokens.update()
            .where(tokens.c.token_symbol.in_(tokens_to_deactivate))
            .values(is_active=False)
        )
        await database.execute(update_query)

    logging.info(f"Deactivated {len(tokens_to_deactivate)} low-activity tokens: {tokens_to_deactivate}")

# ────────────────────────────────────────────
# Delete Tokens Older than 5days
# ────────────────────────────────────────────
async def delete_old_tokens():
    """Delete inactive tokens and their tweets older than 5 days."""
    threshold = datetime.now(timezone.utc) - timedelta(days=5)

    # Get symbols of inactive + old tokens
    old_tokens_query = select(tokens.c.token_symbol).where(
        and_(
            tokens.c.created_at < threshold,
            tokens.c.is_active == False
        )
    )
    rows = await database.fetch_all(old_tokens_query)
    old_symbols = [r["token_symbol"] for r in rows]

    if not old_symbols:
        logging.info("No old tokens to delete.")
        return

    # Step 1: Delete associated tweets
    deleted_tweets = await database.execute(
        delete(tweets).where(tweets.c.token_symbol.in_(old_symbols))
    )

    # Step 2: Delete tokens
    deleted_tokens = await database.execute(
        delete(tokens).where(tokens.c.token_symbol.in_(old_symbols))
    )

    logging.info(f"Deleted {deleted_tokens} tokens and {deleted_tweets} tweets for symbols: {old_symbols}")

# ────────────────────────────────────────────
# Public Entrypoint
# ────────────────────────────────────────────
async def fetch_tokens():
    tokens_data = await get_filtered_pairs()
    if tokens_data:
        await store_tokens(tokens_data)
        fetched_symbols = [t["token_symbol"] for t in tokens_data]
        await update_missing_tokens_info(fetched_symbols) 
    return tokens_data

# ────────────────────────────────────────────
# Fetch From DB for FE Display
# ────────────────────────────────────────────
async def fetch_tokens_from_db():
    rows = await database.fetch_all(tokens.select())

    return [
        {
            "Image": row["image_url"],
            "Token": row["token_symbol"],
            "Age": row["age"],
            "Volume": row["volume_usd"],
            "Liquidity": row["liquidity_usd"],
            "MarketCap": row["market_cap_usd"],
            "dex_url": row["dex_url"],
            "priceChange1h": row["pricechange1h"],
            "WomScore": row["wom_score"],
            "TweetCount": row["tweet_count"],
            "IsBelieve": row["is_believe"],
        }
        for row in rows
    ]

# ────────────────────────────────────────────
# Get token info from DEX
# ────────────────────────────────────────────

DEX_PROXY_URL = os.getenv("DEX_PROXY_URL")
DEX_PROXY_SECRET = os.getenv("DEX_PROXY_SECRET")

async def fetch_token_info_by_address(token_address: str, chain_id: str = "solana") -> dict | None:
    params = {
        "tokenAddresses": token_address,
    }

    headers = {
        "x-secret": DEX_PROXY_SECRET
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{DEX_PROXY_URL}/tokens/v1/{chain_id}",
                headers=headers,
                params=params
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list) and data:
                return data[0]
        except Exception as e:
            logging.error(f"Failed to fetch token info for {token_address}: {e}")
            return None

MAX_CONCURRENT_REQUESTS = 10  # Control concurrency (tune based on your API limit)
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

async def update_missing_tokens_info(fetched_token_symbols):
    db_tokens_query = select(tokens.c.token_symbol, tokens.c.address).where(tokens.c.is_active == True)
    db_tokens = await database.fetch_all(db_tokens_query)

    # Find tokens in DB not recently fetched
    missing_tokens = [t for t in db_tokens if t["token_symbol"] not in fetched_token_symbols]

    logging.info(f"{len(missing_tokens)} tokens missing info: {[t['token_symbol'] for t in missing_tokens]}")

    # Launch concurrent tasks with limited concurrency
    tasks = [
        update_token_task(token["token_symbol"], token["address"])
        for token in missing_tokens
    ]

    await asyncio.gather(*tasks)


async def update_token_task(symbol: str, address: str):
    async with semaphore:
        logging.info(f"Fetching info for token {symbol} ({address})...")

        token_info = await fetch_token_info_by_address(address)

        if not token_info:
            logging.warning(f"Failed to fetch info for {symbol} ({address})")
            return

        await update_token_in_db(address, token_info)
        logging.info(f"Successfully updated {symbol} ({address})")


async def update_token_in_db(address: str, token_info: dict):
    update_query = tokens.update().where(tokens.c.address == address).values(
        volume_usd=token_info.get("volume", {}).get("h24", 0),
        liquidity_usd=token_info.get("liquidity", {}).get("usd", 0),
        market_cap_usd=token_info.get("marketCap", 0),
        pricechange1h=token_info.get("priceChange", {}).get("h1", 0),
        last_seen_at=datetime.now(timezone.utc),
    )
    await database.execute(update_query)
