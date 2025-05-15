import asyncio
from datetime import datetime, timedelta, timezone
import logging
import os
import httpx
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func
from sqlalchemy import and_, delete, select, or_
from db import database
from models import tokens, tweets
import re
import unicodedata

# Logging setup
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)

# Load environment variables
load_dotenv()
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

# ────────────────────────────────────────────
# Token Extraction
# ────────────────────────────────────────────
async def extract_and_format_symbol(raw: str) -> tuple[str, bool]:
    try:
        lines = [line.strip() for line in raw.strip().split("\n") if line.strip()]
        is_believe = any(line.upper() == "DYN" for line in lines)

        ignore_words = {"DLMM", "CLMM", "CPMM", "SOL", "USDC", "/", "", "TRUMP", "DYN"}

        # Remove lines that are:
        # - known labels (DLMM, SOL, etc)
        # - numeric (e.g. "#1")
        # - comments (start with "#")
        # - or explicitly "DYN" (so we don’t use it as a symbol)
        candidates = [
            line
            for line in lines
            if line.upper() not in ignore_words
            and not line.startswith("#")
            and not line.isdigit()
        ]

        if not candidates:
            raise ValueError(f"No valid token symbol candidates in: {raw}")

        symbol = candidates[0].lstrip("$").strip()

        return f"${symbol.lower()}", is_believe

    except Exception as e:
        logging.error(f"Failed to parse token symbol: {raw} – {e}")
        return "$unknown", False

# ────────────────────────────────────────────
# Fetch & Filter Tokens
# ────────────────────────────────────────────

# -- Helper to validate token symbols --
def is_valid_token_symbol(symbol: str) -> bool:
    symbol_clean = symbol.lstrip("$").strip()

    # Must be at least 3 and at most 15 characters
    if not (3 <= len(symbol_clean) <= 15):
        return False

    # Reject if it contains non-letters (no digits, symbols, emojis)
    if not symbol_clean.isalpha():
        return False

    # Extra check for unicode symbols like emojis
    for char in symbol_clean:
        if unicodedata.category(char).startswith("So"):
            return False

    return True

# -- Main function to get filtered tokens --
async def get_filtered_pairs():
    run_input = {
        "chainName": "solana",
        "filterArgs": [
            "?rankBy=trendingScoreH6&order=desc&chainIds=solana&dexIds=meteora,raydium,pumpswap,pumpfun&minLiq=50000&minMarketCap=200000&maxAge=48&min24HVol=150000"
        ],
        "fromPage": 1,
        "toPage": 1,
    }

    filtered_tokens = []
    seen_symbols = set()

    async with httpx.AsyncClient() as client:
        # Start Apify run
        response = await client.post(
            f"https://api.apify.com/v2/acts/crypto-scraper~dexscreener-tokens-scraper/runs?token={api_token}",
            json=run_input
        )
        run_id = response.json()["data"]["id"]

        # Wait for Apify to complete
        while True:
            status_resp = await client.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}?token={api_token}"
            )
            status = status_resp.json()["data"]["status"]
            if status == "SUCCEEDED":
                break
            if status in ["FAILED", "TIMED_OUT", "ABORTED"]:
                raise RuntimeError(f"Apify run failed with status: {status}")
            await asyncio.sleep(5)

        # Get result items
        dataset_id = status_resp.json()["data"]["defaultDatasetId"]
        data_resp = await client.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={api_token}"
        )
        items = data_resp.json()

        logging.info(f"Apify returned {len(items)} items. Filtering...")

        for item in items:
            raw_symbol = item.get("tokenSymbol", "")
            symbol_with_dollar, is_believe = await extract_and_format_symbol(raw_symbol)

            logging.debug(f"Parsed: {raw_symbol} → {symbol_with_dollar}, believe={is_believe}")

            # Validate the symbol
            if not is_valid_token_symbol(symbol_with_dollar):
                logging.info(f"Skipping invalid symbol: {symbol_with_dollar}")
                continue

            if symbol_with_dollar in seen_symbols:
                continue
            seen_symbols.add(symbol_with_dollar)

            filtered_tokens.append({
                "token_symbol": symbol_with_dollar,
                "token_name": item.get("tokenName", "Unknown"),
                "address": item.get("address", "N/A"),
                "age_hours": item.get("age", 0),
                "volume_usd": item.get("volumeUsd", 0),
                "maker_count": item.get("makerCount", 0),
                "liquidity_usd": item.get("liquidityUsd", 0),
                "market_cap_usd": item.get("marketCapUsd", 0),
                "priceChange1h": item.get("priceChange1h", 0),
                "is_believe": is_believe,
            })

        logging.info(f"Filtered {len(filtered_tokens)} tokens: {[t['token_symbol'] for t in filtered_tokens]}")
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
            address=token.get("address"),
            age_hours=token.get("age_hours"),
            volume_usd=token.get("volume_usd"),
            maker_count=token.get("maker_count"),
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
                "age_hours": insert_stmt.excluded.age_hours,
                "volume_usd": insert_stmt.excluded.volume_usd,
                "maker_count": insert_stmt.excluded.maker_count,
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

# ────────────────────────────────────────────
# Deactivation of Inactive Tokens:
# - Created more than 3 hours ago AND total tweet_count < 20
# - Created more than 24 hours ago AND volume_usd < 200,000
# - Created more than 23 hours ago AND fewer than 10 tweets in the last 24h
# ────────────────────────────────────────────

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

    # Step 2: Get all active tokens
    active_tokens_query = select([
        tokens.c.token_symbol,
        tokens.c.address,
        tokens.c.created_at,
        tokens.c.tweet_count,
        tokens.c.volume_usd
    ]).where(tokens.c.is_active == True)
    tokens_data = await database.fetch_all(active_tokens_query)

    # Step 3: Filter tokens to deactivate
    tokens_to_deactivate = []

    for token in tokens_data:
        symbol = token["token_symbol"]
        created_at = token["created_at"]
        tweet_count_total = token["tweet_count"]
        tweet_count_24h = tweet_count_map.get(symbol, 0)
        volume = token["volume_usd"]

        age_hours = (now - created_at).total_seconds() / 3600

        if (
            (age_hours > 3 and tweet_count_total < 20)
            or (age_hours > 24 and volume < 200_000)
            or (age_hours > 23 and tweet_count_24h < 10)
        ):
            tokens_to_deactivate.append(symbol)

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

    await deactivate_low_activity_tokens()
    await delete_old_tokens()

    return tokens_data


# ────────────────────────────────────────────
# Fetch From DB for FE Display
# ────────────────────────────────────────────
async def fetch_tokens_from_db():
    rows = await database.fetch_all(tokens.select())

    return [
        {
            "Token": row["token_symbol"],
            "Age": row["age_hours"],
            "Volume": row["volume_usd"],
            "MakerCount": row["maker_count"],
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

async def fetch_token_info_by_pair_address(pair_id: str, chain_id: str = "solana") -> dict | None:
    params = {
        "pair": pair_id,
        "chain": chain_id,
    }

    headers = {
        "x-secret": DEX_PROXY_SECRET
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(DEX_PROXY_URL, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data.get("pair")
        except Exception as e:
            logging.error(f"Failed to fetch token info for {pair_id}: {e}")
            return None

async def update_missing_tokens_info(fetched_token_symbols):
    db_tokens_query = select(tokens.c.token_symbol, tokens.c.address).where(tokens.c.is_active == True)
    db_tokens = await database.fetch_all(db_tokens_query)

    # Find tokens in DB not recently fetched
    missing_tokens = [t for t in db_tokens if t["token_symbol"] not in fetched_token_symbols]

    logging.info(f"{len(missing_tokens)} tokens missing info: {[t['token_symbol'] for t in missing_tokens]}")

    for token in missing_tokens:
        address = token["address"]
        symbol = token["token_symbol"]

        logging.info(f"Fetching info for token {symbol} ({address})...")

        token_info = await fetch_token_info_by_pair_address(address)

        if not token_info:
            logging.warning(f"Failed to fetch info for {symbol} ({address})")
            continue

        await update_token_in_db(address, token_info)
        logging.info(f"Successfully updated {symbol} ({address})")


async def update_token_in_db(address: str, token_info: dict):
    token_symbol=f'${token_info["baseToken"]["symbol"]}',
    update_query = tokens.update().where(tokens.c.address == address).values(
        token_symbol=token_symbol,
        token_name=token_info["baseToken"]["name"],
        dex_url=token_info["url"],
        volume_usd=token_info.get("volume", {}).get("h24", 0),
        liquidity_usd=token_info.get("liquidity", {}).get("usd", 0),
        market_cap_usd=token_info.get("marketCap", 0),
        pricechange1h=token_info.get("priceChange", {}).get("h1", 0),
        last_seen_at=datetime.now(timezone.utc),
    )
    await database.execute(update_query)
