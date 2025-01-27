import os
import pandas as pd
from apify_client import ApifyClient
from openai import OpenAI
from dotenv import load_dotenv
from utils import preprocess_tweet,is_relevant_tweet

# Load environment variables
load_dotenv()

# Get the API tokens from the environment
apify_api_token = os.getenv("APIFY_API_TOKEN")
deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")

if not apify_api_token or not deepseek_api_key:
    raise ValueError("API tokens not found in environment variables!")

# Initialize the Apify client
client = ApifyClient(apify_api_token)

# Initialize the DeepSeek client
deepseek_client = OpenAI(api_key=deepseek_api_key, base_url="https://api.deepseek.com")


def calculate_bullishness(tweet_text):
    """Analyze sentiment and calculate bullishness percentage using DeepSeek API."""
    try:
        response = deepseek_client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {
                    "role": "system",
                    "content": "You are a financial sentiment analysis assistant. Analyze the sentiment of the following tweet and return a bullishness score between 0 and 100, where 0 is extremely bearish and 100 is extremely bullish.",
                },
                {"role": "user", "content": f"Tweet: {tweet_text}"},
            ],
            max_tokens=10,
            temperature=0.2,
        )

        # Extract the response and convert to a bullishness score
        response_text = response.choices[0].message.content.strip()
        try:
            # Extract a numerical value from the response
            bullishness_score = float(response_text)
            return max(0, min(100, bullishness_score))  # Ensure score is between 0 and 100
        except ValueError:
            # Fallback if the response is not a number
            return 50  # Neutral score as fallback

    except Exception as e:
        print(f"Error analyzing tweet with DeepSeek API: {e}")
        return 50  # Neutral score as fallback


def analyze_cashtags(cashtags):
    """Analyze sentiment for a list of cashtags."""
    data = []

    for cashtag in cashtags:
        search_term = cashtag[1:] if len(cashtag) > 6 else cashtag

        run_input = {
            "searchTerms": [search_term],
            "maxItems": 50,
            "sort": "Latest",
            "tweetLanguage": "en",
        }

        try:
            run = client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input)
            bullishness_scores = []

            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                raw_tweet = item.get("text", "").strip()
                tweet_text = preprocess_tweet(raw_tweet)
                created_at = item.get("createdAt")  # Extract tweet timestamp

                if not is_relevant_tweet(tweet_text):
                    continue

                author_info = item.get("author", {})
                followers_count = author_info.get("followers", 0)
                if followers_count < 150:
                    continue  # Skip this tweet

                # Calculate bullishness using DeepSeek API
                score = calculate_bullishness(tweet_text)
                bullishness_scores.append(score)

                # Append tweet data
                data.append(
                    {
                        "Cashtag": cashtag,
                        "Bullishness": score,
                        "created_at": created_at,  # Include timestamp
                        "Tweet_Text": raw_tweet,  # Include original tweet text
                    }
                )

            # Calculate average bullishness
            if bullishness_scores:
                avg_bullishness = round(sum(bullishness_scores) / len(bullishness_scores), 2)
                data.append({"Cashtag": cashtag, "Bullishness": avg_bullishness})

        except Exception as e:
            print(f"Error analyzing cashtag {cashtag}: {e}")
            data.append({"Cashtag": cashtag, "Bullishness": "Error"})

    return pd.DataFrame(data)
