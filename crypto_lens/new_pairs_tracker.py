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
MAX_AGE = 24  # Maximum age in hours

# Process and filter results
print("Filtered Tokens:")
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    token_name = item.get("tokenName", "Unknown")
    token_symbol = item.get("tokenSymbol", "Unknown").split()[0].strip()  # Extract the first word
    price = item.get("priceUsd", 0)
    age = item.get("age", None)  # Age in hours
    transaction_count = item.get("transactionCount", 0)
    volume_usd = item.get("volumeUsd", 0)
    maker_count = item.get("makerCount", 0)
    liquidity_usd = item.get("liquidityUsd", 0)
    market_cap_usd = item.get("marketCapUsd", 0)
    address = item.get("address", "N/A")

    # Apply filtering criteria
    if (
        age is not None and age <= MAX_AGE  # Filter by age
        and maker_count >= MIN_MAKERS
        and volume_usd >= MIN_VOLUME
        and market_cap_usd >= MIN_MARKET_CAP
        and liquidity_usd >= MIN_LIQUIDITY
    ):
        # Print tokens that meet the criteria
        print(f"Token: {token_name} ({token_symbol})")
        print(f"Address: {address}")
        print(f"Price (USD): ${price:.4f}")
        print(f"Age (hours): {age}")
        print(f"Transactions: {transaction_count}")
        print(f"Volume (USD): ${volume_usd:,}")
        print(f"Maker Count: {maker_count}")
        print(f"Liquidity (USD): ${liquidity_usd:,}")
        print(f"Market Cap (USD): ${market_cap_usd:,}\n")
