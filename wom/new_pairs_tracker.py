import asyncio
import os
import logging
import httpx
from dotenv import load_dotenv
from models import tokens
from db import database
from sqlalchemy.dialects.postgresql import insert

logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)
load_dotenv()
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

DISK_PATH = os.getenv("DISK_PATH", "/tmp")  # fallback for local/testing
DB_PATH = os.path.join(DISK_PATH, "tokens.db")

if not os.path.exists(DISK_PATH) and not DISK_PATH.startswith("/data"):
    os.makedirs(DISK_PATH, exist_ok=True)

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
    MAX_AGE = 24  # hours

    filtered_tokens = []
    unique_symbols = set()

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"https://api.apify.com/v2/acts/crypto-scraper~dexscreener-tokens-scraper/runs?token={api_token}",
            json=run_input,
        )
        response.raise_for_status()
        run_id = response.json()["data"]["id"]

        # Wait for the run to complete
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

        # Fetch dataset items
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
            token_symbol = await extract_and_format_symbol(token_symbol_raw)
            age = item.get("age", None)
            volume_usd = item.get("volumeUsd", 0)
            maker_count = item.get("makerCount", 0)
            liquidity_usd = item.get("liquidityUsd", 0)
            market_cap_usd = item.get("marketCapUsd", 0)
            priceChange1h = item.get("priceChange1h", 0)
            address = item.get("address", "N/A")

            if (age is not None and age <= MAX_AGE and
                maker_count >= MIN_MAKERS ):
                if token_symbol not in unique_symbols:
                    unique_symbols.add(token_symbol)
                    filtered_tokens.append({
                        "token_name": token_name,
                        "token_symbol": token_symbol,
                        "address": address,
                        "age_hours": age,
                        "volume_usd": volume_usd,
                        "maker_count": maker_count,
                        "liquidity_usd": liquidity_usd,
                        "market_cap_usd": market_cap_usd,
                        "priceChange1h" : priceChange1h,
                        "address" : address
                    })
    logging.info(f"Filtering complete. Total unique tokens: {len(filtered_tokens)}.")
    return filtered_tokens

async def store_tokens(tokens_data):
    """
    Store tokens into the PostgreSQL 'tokens' table using async insert/update.
    """
    query = insert(tokens).on_conflict_do_update(
        index_elements=["token_symbol"],
        set_={
            "token_name": tokens.c.token_name,
            "address": tokens.c.address,
            "age_hours": tokens.c.age_hours,
            "volume_usd": tokens.c.volume_usd,
            "maker_count": tokens.c.maker_count,
            "liquidity_usd": tokens.c.liquidity_usd,
            "market_cap_usd": tokens.c.market_cap_usd,
            "dex_url": tokens.c.dex_url,
            "pricechange1h": tokens.c.pricechange1h,
        }
    )

    values = []
    for token in tokens_data:
        values.append({
            "token_symbol": token.get("token_symbol"),
            "token_name": token.get("token_name"),
            "address": token.get("address"),
            "age_hours": token.get("age_hours"),
            "volume_usd": token.get("volume_usd"),
            "maker_count": token.get("maker_count"),
            "liquidity_usd": token.get("liquidity_usd"),
            "market_cap_usd": token.get("market_cap_usd"),
            "dex_url": f"https://dexscreener.com/solana/{token.get('address')}",
            "pricechange1h": token.get("priceChange1h"), 
        })

    await database.execute_many(query=query, values=values)
    logging.info("Tokens stored/updated in PostgreSQL successfully.")


async def fetch_tokens():
    """
    Pipeline: Fetch filtered tokens from Apify, store the tokens, and return them.
    """
    filtered_tokens = await get_filtered_pairs()
    if filtered_tokens:
        await store_tokens(filtered_tokens)
    else:
        logging.info("No tokens with recent Raydium pools to store.")
    return filtered_tokens
