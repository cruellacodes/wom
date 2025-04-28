import asyncio
from datetime import datetime, timedelta, timezone
import logging
import os
import httpx
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_, delete, select, or_
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

    # MIN_VOLUME = 200_000
    # MIN_MARKET_CAP = 250_000
    # MIN_LIQUIDITY = 100_000
    #MAX_AGE = 48  # hours

    filtered_tokens = []
    unique_symbols = set()
    #MIN_MAKERS = 5000
    #MAX_AGE = 48

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

            if (
                token_symbol not in unique_symbols
            ):
                unique_symbols.add(token_symbol)
                filtered_tokens.append({
                    "token_symbol": token_symbol,
                    "token_name": item.get("tokenName", "Unknown"),
                    "address": item.get("address", "N/A"),
                    "age_hours": item.get("age", 0),
                    "volume_usd": item.get("volumeUsd", 0),
                    "maker_count": item.get("makerCount", 0),
                    "liquidity_usd": item.get("liquidityUsd", 0),
                    "market_cap_usd": item.get("marketCapUsd", 0),
                    "priceChange1h": item.get("priceChange1h", 0)
                })
        logging.info(f"Filtered {len(filtered_tokens)} tokens: {filtered_tokens}")
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
            }
        )

        await database.execute(update_stmt)

    logging.info(f"Stored/Updated {len(tokens_data)} tokens.")

# ────────────────────────────────────────────
# Deactivation of Inactive Tokens
# ────────────────────────────────────────────

async def deactivate_low_activity_tokens():
    three_hours_ago = datetime.now(timezone.utc) - timedelta(hours=3)

    query = tokens.update().where(
        and_(
            tokens.c.is_active == True,
            or_(
                and_(
                    tokens.c.tweet_count < 20,
                    tokens.c.created_at < three_hours_ago,
                ),
                and_(
                    tokens.c.age_hours > 24,
                    tokens.c.volume_usd < 200000
                )
            )
        )
    ).values(is_active=False)

    count = await database.execute(query)
    logging.info(f"Marked {len(count)} low-activity tokens as inactive.")

# ────────────────────────────────────────────
# Delete Tokens Older than 5days
# ────────────────────────────────────────────
async def delete_old_tokens():
    """Delete tokens (and their tweets) older than 5 days."""
    threshold = datetime.now(timezone.utc) - timedelta(days=5)

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
            "TweetCount": row["tweet_count"]
        }
        for row in rows
    ]

# ────────────────────────────────────────────
# Update tokens info from DEX
# ────────────────────────────────────────────
async def update_missing_tokens_info(fetched_token_symbols):
    logging.info("Checking for tokens needing info refresh (BATCH mode)...")

    # Step 1: Get all tokens from the database
    db_tokens_query = select(tokens.c.token_symbol, tokens.c.address).where(tokens.c.is_active == True)
    db_tokens = await database.fetch_all(db_tokens_query)

    # Step 2: Find tokens missing from the latest fetch
    missing_tokens = [
        t for t in db_tokens
        if t["token_symbol"] not in fetched_token_symbols
    ]

    if not missing_tokens:
        logging.info("No tokens need updating.")
        return

    async with httpx.AsyncClient() as client:
        # Step 3: Batch tokens 30-by-30
        batch_size = 30
        for i in range(0, len(missing_tokens), batch_size):
            batch = missing_tokens[i:i + batch_size]
            token_addresses = ",".join(t["address"] for t in batch)

            try:
                chain_id = "solana"
                url = f"https://api.dexscreener.com/tokens/v1/{chain_id}/{token_addresses}"
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()

                if not isinstance(data, list):
                    logging.warning(f"No info found for batch starting with {batch[0]['token_symbol']}")
                    continue

                # Step 4: Update each token info
                for token_info in data:
                    base_token = token_info.get("baseToken", {})
                    address = base_token.get("address")

                    if not address:
                        continue

                    volume_usd = token_info.get("volume", {}).get("h24", 0)
                    liquidity_usd = token_info.get("liquidity", {}).get("usd", 0)
                    price_change_1h = token_info.get("priceChange", {}).get("h1", 0)
                    market_cap = token_info.get("market_cap", 0)

                    update_query = tokens.update().where(
                        tokens.c.address == address
                    ).values(
                        volume_usd=volume_usd,
                        liquidity_usd=liquidity_usd,
                        pricechange1h=price_change_1h,
                        last_seen_at=datetime.now(timezone.utc),
                    )

                    await database.execute(update_query)
                    logging.info(f"Updated token info for address {address} successfully.")

            except Exception as e:
                logging.error(f"Error updating batch starting with {batch[0]['token_symbol']}: {e}")
