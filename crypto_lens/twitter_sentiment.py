from dotenv import load_dotenv
import os
from apify_client import ApifyClient

# Load environment variables from .env
load_dotenv()

# Get the API token from the environment
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("API token not found in environment variables!")

# Initialize the ApifyClient
client = ApifyClient(api_token)


# Prepare the Actor input for cashtags
run_input = {
    "searchTerms": [
        "$LLM",  
    ],
    "maxItems": 100,
    "sort": "Latest",
    "tweetLanguage": "en",
}

# Run the Actor
run = client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input)

# Fetch and process results
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    tweet_text = item.get("text", "")
    if tweet_text:
        # Display the results
        print(f"User: {item.get('username')}")
        print(f"Tweet: {tweet_text}")
        print(f"Retweets: {item.get('retweetsCount')}")
        print(f"Likes: {item.get('favoritesCount')}")
        print(f"Date: {item.get('createdAt')}")
        print("-" * 50)
