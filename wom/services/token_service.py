import asyncio
from datetime import datetime, timedelta, timezone
import logging
import os
import httpx
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_, delete, select
from db import database
from models import tokens, tweets

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
async def extract_and_format_symbol(raw: str) -> str:
    try:
        parts = raw.split()
        symbol = parts[2] if len(parts) > 1 and parts[1] in ["DLMM", "CLMM", "CPMM"] else parts[1]
        return f"${symbol.strip()}"
    except (IndexError, AttributeError) as e:
        logging.error(f"Failed to parse token symbol: {raw} – {e}")
        return "$Unknown"

# ────────────────────────────────────────────
# Fetch & Filter Tokens
# ────────────────────────────────────────────
async def get_filtered_pairs():
    run_input = {
        "chainName": "solana",
        "filterArgs": [
            "?rankBy=trendingScoreH6&order=desc&chainIds=solana&dexIds=raydium,pumpswap,pumpfun&minLiq=50000&minMarketCap=200000&maxAge=48&min24HVol=150000"
        ],
        "fromPage": 1,
        "toPage": 1,
    }

    MIN_MAKERS = 7000
    # MIN_VOLUME = 200_000
    # MIN_MARKET_CAP = 250_000
    # MIN_LIQUIDITY = 100_000
    MAX_AGE = 48  # hours

    filtered_tokens = []
    unique_symbols = set()
    MIN_MAKERS = 7000
    MAX_AGE = 48

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"https://api.apify.com/v2/acts/crypto-scraper~dexscreener-tokens-scraper/runs?token={api_token}",
            json=run_input
        )
        run_id = response.json()["data"]["id"]

        # Wait for Apify to finish
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

        dataset_id = status_resp.json()["data"]["defaultDatasetId"]
        data_resp = await client.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={api_token}"
        )
        items = data_resp.json()

        logging.info("Filtering fetched token data...")

        for item in items:
            token_symbol = await extract_and_format_symbol(item.get("tokenSymbol", ""))
            token_symbol = token_symbol.lower()

            age = item.get("age")
            maker_count = item.get("makerCount", 0)

            if (
                age is not None and
                age <= MAX_AGE and
                maker_count >= MIN_MAKERS and
                token_symbol not in unique_symbols
            ):
                unique_symbols.add(token_symbol)
                filtered_tokens.append({
                    "token_symbol": token_symbol,
                    "token_name": item.get("tokenName", "Unknown"),
                    "address": item.get("address", "N/A"),
                    "age_hours": age,
                    "volume_usd": item.get("volumeUsd", 0),
                    "maker_count": maker_count,
                    "liquidity_usd": item.get("liquidityUsd", 0),
                    "market_cap_usd": item.get("marketCapUsd", 0),
                    "priceChange1h": item.get("priceChange1h", 0)
                })
        logging.info(f"Filtered {len(filtered_tokens)} tokens.")
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
            tweet_count=0
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
                "wom_score": insert_stmt.excluded.wom_score,
                "tweet_count": insert_stmt.excluded.tweet_count
            }
        )

        await database.execute(update_stmt)

    logging.info(f"Stored/Updated {len(tokens_data)} tokens.")

# ────────────────────────────────────────────
# Deactivation of Inactive Tokens
# ────────────────────────────────────────────
async def deactivate_low_activity_tokens():
    query = tokens.update().where(
        and_(
            tokens.c.tweet_count < 20,
            tokens.c.age_hours > 3,
            tokens.c.is_active == True
        )
    ).values(is_active=False)

    count = await database.execute(query)
    logging.info(f"Marked {count} low-activity tokens as inactive.")

# ────────────────────────────────────────────
# Delete Tokens Older than 48h
# ────────────────────────────────────────────
async def delete_old_tokens():
    threshold = datetime.now(timezone.utc) - timedelta(hours=48)

    old_tokens_query = select(tokens.c.token_symbol).where(tokens.c.created_at < threshold)
    rows = await database.fetch_all(old_tokens_query)
    old_symbols = [r["token_symbol"] for r in rows]

    if not old_symbols:
        return

    deleted_tweets = await database.execute(
        delete(tweets).where(tweets.c.token_symbol.in_(old_symbols))
    )
    deleted_tokens = await database.execute(
        delete(tokens).where(tokens.c.token_symbol.in_(old_symbols))
    )

    logging.info(f"Deleted {deleted_tokens} old tokens and {deleted_tweets} tweets.")

# ────────────────────────────────────────────
# Public Entrypoint
# ────────────────────────────────────────────
async def fetch_tokens():
    tokens_data = await get_filtered_pairs()
    if tokens_data:
        await store_tokens(tokens_data)
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
            "TweetCount": row["tweet_count"]
        }
        for row in rows
    ]