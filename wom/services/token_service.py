import asyncio
from datetime import datetime, timezone
import os
import logging
import httpx
from dotenv import load_dotenv
from models import tokens
from db import database
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import delete
from sqlalchemy import select
from datetime import timedelta
from models import tweets  

logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)
load_dotenv()
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

async def extract_and_format_symbol(token_symbol_raw):
    """Format the token symbol as a cashtag."""
    try:
        parts = token_symbol_raw.split()
        if len(parts) > 1 and parts[1] in ["DLMM", "CLMM", "CPMM"]:
            symbol = parts[2]
        else:
            symbol = parts[1]
        return f"${symbol.strip()}"
    except (IndexError, AttributeError) as e:
        logging.error(f"Error formatting token symbol from '{token_symbol_raw}': {e}")
        return "$Unknown"

async def get_filtered_pairs():
    """Fetch tokens from Apify and apply filtering criteria."""
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

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"https://api.apify.com/v2/acts/crypto-scraper~dexscreener-tokens-scraper/runs?token={api_token}",
            json=run_input,
        )
        response.raise_for_status()
        run_id = response.json()["data"]["id"]

        # Wait for Apify run to complete
        while True:
            run_status = await client.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}?token={api_token}"
            )
            run_status.raise_for_status()
            status = run_status.json()["data"]["status"]
            if status == "SUCCEEDED":
                break
            elif status in ["FAILED", "TIMED_OUT", "ABORTED"]:
                raise RuntimeError(f"Apify run failed with status: {status}")
            await asyncio.sleep(5)

        # Fetch the dataset items
        dataset_id = run_status.json()["data"]["defaultDatasetId"]
        dataset_response = await client.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={api_token}"
        )
        dataset_response.raise_for_status()
        items = dataset_response.json()

        logging.info("Processing and filtering fetched token data.")
        for item in items:
            token_name = item.get("tokenName", "Unknown")
            token_symbol_raw = item.get("tokenSymbol", "Unknown")
            symbol = (await extract_and_format_symbol(token_symbol_raw)).lower()  # Normalize early

            age = item.get("age", None)
            volume_usd = item.get("volumeUsd", 0)
            maker_count = item.get("makerCount", 0)
            liquidity_usd = item.get("liquidityUsd", 0)
            market_cap_usd = item.get("marketCapUsd", 0)
            price_change_1h = item.get("priceChange1h", 0)
            address = item.get("address", "N/A")

            if (
                age is not None and age <= MAX_AGE and
                maker_count >= MIN_MAKERS and
                symbol not in unique_symbols
            ):
                unique_symbols.add(symbol)
                filtered_tokens.append({
                    "token_name": token_name,
                    "token_symbol": symbol,
                    "address": address,
                    "age_hours": age,
                    "volume_usd": volume_usd,
                    "maker_count": maker_count,
                    "liquidity_usd": liquidity_usd,
                    "market_cap_usd": market_cap_usd,
                    "priceChange1h": price_change_1h
                })

    logging.info(f"Filtering complete. Total unique tokens: {len(filtered_tokens)}.")
    return filtered_tokens

async def store_tokens(tokens_data):
    now = datetime.now(timezone.utc)

    for token in tokens_data:
        token_symbol = token.get("token_symbol", "").lower()

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
                "tweet_count": insert_stmt.excluded.tweet_count,
            }
        )

        await database.execute(update_stmt)

    logging.info(f"Stored/Updated {len(tokens_data)} tokens.")

async def deactivate_stale_tokens(grace_period_hours=3):
    """
    Mark tokens as inactive if they haven't been seen in the last `grace_period_hours`.
    """
    threshold = datetime.now(timezone.utc) - timedelta(hours=grace_period_hours)
    query = tokens.update().where(
        tokens.c.last_seen_at < threshold,
        tokens.c.is_active == True
    ).values(is_active=False)
    
    updated = await database.execute(query)
    logging.info(f"[Deactivation] Marked {updated} token(s) as inactive.")


async def fetch_tokens():
    """
    Pipeline: Fetch filtered tokens from Apify, store them, clean up old ones.
    """
    filtered_tokens = await get_filtered_pairs()
    if filtered_tokens:
        await store_tokens(filtered_tokens)
    else:
        logging.info("No tokens with recent Raydium pools to store.")

    await deactivate_stale_tokens(grace_period_hours=3)
    await delete_old_tokens()
    return filtered_tokens

async def fetch_tokens_from_db():
    query = tokens.select()
    rows = await database.fetch_all(query)

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

async def delete_old_tokens():
    """
    Delete tokens older than 48 hours and their associated tweets.
    """
    threshold = datetime.now(timezone.utc) - timedelta(hours=48)

    # Step 1: Find tokens older than 48h
    old_tokens_query = select(tokens.c.token_symbol).where(tokens.c.created_at < threshold)
    old_token_rows = await database.fetch_all(old_tokens_query)
    old_symbols = [row["token_symbol"] for row in old_token_rows]

    if not old_symbols:
        logging.info("[Cleanup] No old tokens found for deletion.")
        return

    # Step 2: Delete tweets associated with those tokens
    delete_tweets_query = delete(tweets).where(tweets.c.token_symbol.in_(old_symbols))
    deleted_tweets = await database.execute(delete_tweets_query)

    # Step 3: Delete the tokens themselves
    delete_tokens_query = delete(tokens).where(tokens.c.token_symbol.in_(old_symbols))
    deleted_tokens = await database.execute(delete_tokens_query)

    logging.info(f"[Cleanup] Deleted {deleted_tokens} old token(s) and {deleted_tweets} associated tweet(s).")