import requests
import logging
from typing import List, Dict
import datetime as dt


# Constants
FETCH_API_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
SEARCH_API_URL = "https://api.dexscreener.com/latest/dex/search"
CHAIN_ID = "solana"  
MIN_MARKET_CAP = 100_000  # Minimum market cap in USD
HEADERS = {"User-Agent": "DexScreenerBot"}  

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def fetch_latest_tokens() -> List[Dict]:
    """Fetch the latest tokens from the API."""
    logging.info("Fetching latest tokens...")
    response = requests.get(FETCH_API_URL, headers=HEADERS)

    if response.status_code != 200:
        logging.error(f"Failed to fetch tokens. Status Code: {response.status_code}")
        return []

    tokens = response.json()
    logging.info(f"Fetched {len(tokens)} tokens.")
    return tokens


def search_token_pairs(token_address: str) -> Dict:
    """Search for token pairs by token address."""
    logging.info(f"Searching pairs for token: {token_address}...")
    response = requests.get(f"{SEARCH_API_URL}?q={token_address}", headers=HEADERS)

    if response.status_code != 200:
        logging.error(f"Failed to search pairs for {token_address}. Status Code: {response.status_code}")
        return {}

    data = response.json()
    return data.get("pairs", [])


def filter_and_display(tokens: List[Dict]) -> None:
    """Filter tokens based on criteria and display results."""
    logging.info("Filtering tokens based on criteria...")
    results = []

    for token in tokens:
        token_address = token.get("tokenAddress")
        if not token_address:
            logging.warning("Token missing `tokenAddress`. Skipping...")
            continue

        # Fetch pair information
        pairs = search_token_pairs(token_address)
        for pair in pairs:
            chain_id = pair.get("chainId")
            market_cap = pair.get("marketCap", 0)
            liquidity_usd = pair.get("liquidity", {}).get("usd", 0)
            pair_url = pair.get("url")
            base_token = pair.get("baseToken", {})
            created_at_timestamp = pair.get("pairCreatedAt")

            # Validate and format `created_at`
            try:
                created_at = (
                    dt.datetime.utcfromtimestamp(created_at_timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")
                    if created_at_timestamp and created_at_timestamp > 0
                    else "Unknown"
                )
            except (TypeError, ValueError, OSError):
                logging.warning(f"Invalid `pairCreatedAt` value: {created_at_timestamp}. Skipping...")
                created_at = "Unknown"

            # Apply filters
            if chain_id == CHAIN_ID and market_cap >= MIN_MARKET_CAP:
                results.append({
                    "token_name": base_token.get("name"),
                    "token_symbol": base_token.get("symbol"),
                    "market_cap": market_cap,
                    "liquidity_usd": liquidity_usd,
                    "url": pair_url,
                    "created_at": created_at,
                })

    # Display results
    if results:
        logging.info(f"Found {len(results)} tokens matching criteria.")
        for result in results:
            logging.info(
                f"\n[INFO] \n"
                f"Token: {result['token_name']} (${result['token_symbol']})\n"
                f"Market Cap: ${result['market_cap']:,}\n"
                f"Liquidity (USD): ${result['liquidity_usd']:,}\n"
                f"URL: {result['url']}\n"
                f"Created At: {result['created_at']}\n"
            )
    else:
        logging.info("No tokens matched the criteria.")



def main():
    tokens = fetch_latest_tokens()
    if tokens:
        filter_and_display(tokens)


if __name__ == "__main__":
    main()
