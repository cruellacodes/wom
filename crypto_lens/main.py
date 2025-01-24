import os
from dotenv import load_dotenv
from twitter_analysis import analyze_cashtags
from new_pairs_tracker import main as fetch_new_pairs

# Load environment variables
load_dotenv()
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("API token not found in environment variables!")

def main():
    # Step 1: Fetch new token pairs
    print("Fetching new token pairs...")
    tokens = fetch_new_pairs()
    if not tokens:
        print("No new token pairs found.")
        return

    # Extract and deduplicate cashtags
    cashtags = list(set(f"${token['token_symbol']}" for token in tokens if token.get("token_symbol")))
    print(f"Found unique cashtags: {cashtags}")

    # Step 2: Perform sentiment analysis on Twitter
    if cashtags:
        print("\nPerforming sentiment analysis...")
        overall_sentiments = analyze_cashtags(cashtags)
        print("\nOverall Sentiment Analysis:")
        for cashtag, sentiment in overall_sentiments.items():
            print(f"{cashtag}: {sentiment}")
    else:
        print("No valid cashtags to analyze.")

if __name__ == "__main__":
    main()
