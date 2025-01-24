import os
from apify_client import ApifyClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from utils import is_relevant_tweet, weighted_sentiment_score
from dotenv import load_dotenv


# Load environment variables from .env
load_dotenv()

# Get the API token from the environment
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

# Initialize the Apify client with the token
client = ApifyClient(api_token)
analyzer = SentimentIntensityAnalyzer()

def analyze_cashtags(cashtags):
    """Search Twitter for cashtags and perform sentiment analysis."""
    overall_sentiments = {}

    for cashtag in cashtags:
        # Determine search term (remove "$" for symbols > 6 chars)
        search_term = cashtag[1:] if len(cashtag) > 7 else cashtag
        print(f"Analyzing tweets for {search_term}...")

        # Prepare Actor input for Twitter scraping
        run_input = {
            "searchTerms": [search_term],
            "maxItems": 50,
            "sort": "Latest",
            "tweetLanguage": "en",
        }

        try:
            # Run the Twitter scraper Actor
            run = client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input)

            # Perform sentiment analysis
            sentiment_scores = []
            engagements = []  # Track likes + retweets for weighting
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                tweet_text = item.get("text", "").strip()
                if not is_relevant_tweet(tweet_text):
                    continue

                sentiment = analyzer.polarity_scores(tweet_text)["compound"]
                sentiment_scores.append(sentiment)
                likes = item.get("favoritesCount", 0)
                retweets = item.get("retweetsCount", 0)
                engagements.append(likes + retweets)

            # Calculate weighted sentiment score
            if sentiment_scores:
                average_sentiment = weighted_sentiment_score(sentiment_scores, engagements)
                sentiment_label = (
                    "Positive" if average_sentiment > 0 else
                    "Negative" if average_sentiment < 0 else
                    "Neutral"
                )
                overall_sentiments[cashtag] = sentiment_label
            else:
                overall_sentiments[cashtag] = "No tweets found"

        except Exception as e:
            print(f"Error analyzing tweets for {search_term}: {e}")
            overall_sentiments[cashtag] = "Error fetching tweets"

    return overall_sentiments
