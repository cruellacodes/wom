import asyncio
import os
import sqlite3
import logging
import httpx
from dotenv import load_dotenv

logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)
load_dotenv()
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

DB_PATH = "tokens.db"

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
            "?rankBy=trendingScoreH6&order=desc&chainIds=solana&dexIds=raydium&minLiq=100000&minMarketCap=250000&maxAge=24&min24HVol=200000"
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


def store_tokens(tokens):
    """
    Store tokens in the 'tokens' table.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    for token in tokens:
        dex_url = f"https://dexscreener.com/solana/{token.get('address')}"
        cursor.execute("""
            INSERT OR REPLACE INTO tokens (
                token_symbol,
                token_name,
                address,
                age_hours,
                volume_usd,
                maker_count,
                liquidity_usd,
                market_cap_usd,
                dex_url,
                priceChange1h
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            token.get("token_symbol"),
            token.get("token_name"),
            token.get("address"),
            token.get("age_hours"),
            token.get("volume_usd"),
            token.get("maker_count"),
            token.get("liquidity_usd"),
            token.get("market_cap_usd"),
            dex_url,
            token.get("priceChange1h"),
        ))
    conn.commit()
    conn.close()
    logging.info("Tokens stored in the database succesfully.")

import sqlite3

def fetch_tokens_from_db(filtered_tokens):
    """
    Fetch only the filtered tokens from the 'tokens' table.
    """
    if not filtered_tokens:
        return []  # Return empty list if no tokens to filter

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    token_addresses = tuple(token["address"] for token in filtered_tokens)

    # Use parameterized query to prevent SQL injection
    query = f"""
        SELECT token_symbol, token_name, address, age_hours, volume_usd, maker_count,
               liquidity_usd, market_cap_usd, dex_url, priceChange1h
        FROM tokens
        WHERE token_addresses IN ({",".join(["?"] * len(token_addresses))})
    """

    cursor.execute(query, token_addresses)
    rows = cursor.fetchall()
    conn.close()

    # Convert fetched rows into a list of dictionaries
    tokens = []
    for row in rows:
        tokens.append({
            "token_symbol": row[0],
            "token_name": row[1],
            "address": row[2],
            "age_hours": row[3],
            "volume_usd": row[4],
            "maker_count": row[5],
            "liquidity_usd": row[6],
            "market_cap_usd": row[7],
            "dex_url": row[8],
            "priceChange1h": row[9]
        })
    
    return tokens


async def fetch_tokens():
    """
    Pipeline: Fetch filtered tokens from Apify, store the tokens, and return them.
    """
    filtered_tokens = await get_filtered_pairs()
    if filtered_tokens:
        store_tokens(filtered_tokens)
    else:
        logging.info("No tokens with recent Raydium pools to store.")
    return filtered_tokens


async def fetch_and_analyze(filtered_tokens):
    """
    Fetch tokens from DB, then run tweet fetching and sentiment analysis.
    This function updates the tokens table with wom_score and tweet_count.
    """
    from twitter_analysis import get_sentiment, fetch_tweets  # Ensure these are accessible

    tokens_from_db = fetch_tokens_from_db(filtered_tokens)
    if not tokens_from_db:
        logging.info("No tokens available in the database.")
        return []

    cashtags = [token['token_symbol'] for token in tokens_from_db]
    logging.info(f"Tokens for sentiment analysis: {cashtags}")

    # Fetch tweets for each token (round-robin over your task IDs)
    import itertools
    task_cycle = itertools.cycle(os.getenv("WORKER_IDS").split(","))
    tweet_tasks = [fetch_tweets(token, next(task_cycle).strip(), DB_PATH) for token in cashtags]
    await asyncio.gather(*tweet_tasks)

    sentiment_dict = await get_sentiment(cashtags, DB_PATH)
    final_results = []
    for token in tokens_from_db:
        ts = token.get("token_symbol")
        wom_score = sentiment_dict.get(ts, {}).get("wom_score", 0)
        tweet_count = sentiment_dict.get(ts, {}).get("tweet_count", 0)
        result = {
            "Token": ts,
            "WomScore": float(wom_score) if wom_score is not None else None,
            "TweetCount": tweet_count,
            "MarketCap": token.get("market_cap_usd"),
            "Age": token.get("age_hours"),
            "Volume": token.get("volume_usd"),
            "MakerCount": token.get("maker_count"),
            "Liquidity": token.get("liquidity_usd"),
            "DexUrl": token.get("dex_url"),
            "priceChange1h": token.get("priceChange1h")
        }
        final_results.append(result)

    # Update the tokens table with the sentiment values.
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    for token in final_results:
        cursor.execute("""
            UPDATE tokens
            SET wom_score = ?, tweet_count = ?
            WHERE token_symbol = ?
        """, (token["WomScore"], token["TweetCount"], token["Token"]))
    conn.commit()
    conn.close()

    return final_results
