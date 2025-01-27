import os
from apify_client import ApifyClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get the API token from the environment
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

# Initialize the Apify client with the token
client = ApifyClient(api_token)


def extract_and_format_symbol(token_symbol_raw):
    """
    Extract the token symbol and format it as a cashtag.
    Handles various inconsistent formats in the tokenSymbol field.
    Args:
        token_symbol_raw (str): Raw tokenSymbol string, e.g., '#1 GRNLD / SOL Hi Greenland 11500'.
    Returns:
        str: Formatted token symbol, e.g., '$GRNLD'.
    """
    try:
        # Split the string by line breaks and spaces, clean up, and find the symbol
        parts = token_symbol_raw.split("\n")[1].split(" ")[0].strip()
        return f"${parts}" if parts else "$Unknown"
    except (IndexError, AttributeError):
        return "$Unknown"



def get_filtered_pairs():
    """
    Fetch tokens from Apify, filter them based on the criteria, and return the filtered pairs.
    Returns:
        List[Dict]: A list of filtered tokens.
    """
    # Prepare the Actor input
    run_input = {
        "chainName": "solana",
        "filterArgs": [
            "?rankBy=trendingScoreH6&order=desc&minLiq=100000&minMarketCap=250000&min12HVol=200000"
        ],
        "fromPage": 1,
        "toPage": 1,
    }

    # Run the Actor and wait for it to finish
    run = client.actor("GWfH8uzlNFz2fEjKj").call(run_input=run_input)

    # Filter criteria
    MIN_MAKERS = 500
    MIN_VOLUME = 200_000
    MIN_MARKET_CAP = 250_000
    MIN_LIQUIDITY = 100_000
    MAX_AGE = 24  # hours

    # Process and filter results
    filtered_tokens = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        token_name = item.get("tokenName", "Unknown")
        token_symbol_raw = item.get("tokenSymbol", "Unknown")

        # Format the token symbol
        token_symbol = extract_and_format_symbol(token_symbol_raw)

        age = item.get("age", None)  # Age in hours
        volume_usd = item.get("volumeUsd", 0)
        maker_count = item.get("makerCount", 0)
        liquidity_usd = item.get("liquidityUsd", 0)
        market_cap_usd = item.get("marketCapUsd", 0)
        address = item.get("address", "N/A")

        # Apply filtering criteria
        if (
            age is not None and age <= MAX_AGE
            and maker_count >= MIN_MAKERS
            and volume_usd >= MIN_VOLUME
            and market_cap_usd >= MIN_MARKET_CAP
            and liquidity_usd >= MIN_LIQUIDITY
        ):
            # Add the token to the filtered list
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

    return filtered_tokens
