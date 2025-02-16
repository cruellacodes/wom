import asyncio
import os
import logging
import httpx
from dotenv import load_dotenv

# Configure logging with a professional format
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)

# Load environment variables
load_dotenv()

# Get the API token from the environment
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

async def extract_and_format_symbol(token_symbol_raw):
    """
    Extract the token symbol and format it as a cashtag.
    Handles various inconsistent formats in the tokenSymbol field.
    Args:
        token_symbol_raw (str): Raw tokenSymbol string, e.g., '#24 DLMM TRUMP / USDC OFFICIAL TRUMP'.
    Returns:
        str: Formatted token symbol, e.g., '$TRUMP'.
    """
    try:
        # Split the string by spaces
        parts = token_symbol_raw.split()
        # Check for DLMM, CLMM, or CPMM prefixes and skip them if present
        if len(parts) > 1 and parts[1] in ["DLMM", "CLMM", "CPMM"]:
            symbol = parts[2]
        else:
            symbol = parts[1]
        formatted_symbol = f"${symbol.strip()}"
        return formatted_symbol
    except (IndexError, AttributeError) as e:
        logging.error(f"Error formatting token symbol from '{token_symbol_raw}': {e}")
        return "$Unknown"

async def get_filtered_pairs():
    """
    Fetch tokens from Apify, filter them based on the criteria, and return unique filtered pairs.
    Returns:
        List[Dict]: A list of filtered tokens.
    """
    run_input = {
        "chainName": "solana",
        "filterArgs": [
            "?rankBy=trendingScoreH6&order=desc&minLiq=100000&minMarketCap=250000&min12HVol=200000"
        ],
        "fromPage": 1,
        "toPage": 1,
    }

    # Filtering criteria
    MIN_MAKERS = 7000
    MIN_VOLUME = 200_000
    MIN_MARKET_CAP = 250_000
    MIN_LIQUIDITY = 100_000
    MAX_AGE = 24  # hours

    filtered_tokens = []
    unique_symbols = set()  # Track unique symbols

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
            await asyncio.sleep(5)  # Wait 5 seconds before checking again

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

            # Format the token symbol
            token_symbol = await extract_and_format_symbol(token_symbol_raw)

            age = item.get("age", None)  # Age in hours
            volume_usd = item.get("volumeUsd", 0)
            maker_count = item.get("makerCount", 0)
            liquidity_usd = item.get("liquidityUsd", 0)
            market_cap_usd = item.get("marketCapUsd", 0)
            address = item.get("address", "N/A")

            # Apply filtering criteria
            if (
                age is not None and age <= MAX_AGE and
                maker_count >= MIN_MAKERS and
                volume_usd >= MIN_VOLUME and
                market_cap_usd >= MIN_MARKET_CAP and
                liquidity_usd >= MIN_LIQUIDITY
            ):
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
                    })

    logging.info(f"Filtering complete. Total unique tokens: {len(filtered_tokens)}.")

    return filtered_tokens
