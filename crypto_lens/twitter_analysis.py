import os
from apify_client import ApifyClient
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
from utils import preprocess_tweet, is_relevant_tweet
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get the API token from the environment
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

# Initialize the Apify client with the token
client = ApifyClient(api_token)

# Load FinBERT model and tokenizer
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")


def calculate_bullishness(tweet_text):
    """Analyze sentiment and calculate bullishness percentage using FinBERT."""
    inputs = tokenizer(tweet_text, return_tensors="pt", truncation=True, padding=True)
    outputs = model(**inputs)
    probs = softmax(outputs.logits.detach().numpy()[0])  # Convert logits to probabilities

    # Explicitly round probabilities
    bullish_prob = round(probs[0] * 100, 2)
    neutral_prob = round(probs[1] * 100, 2)
    bearish_prob = round(probs[2] * 100, 2)

    # Calculate bullishness score
    bullishness_score = round(bullish_prob / (bullish_prob + bearish_prob), 2)
    return {
        "bullish": bullish_prob,
        "neutral": neutral_prob,
        "bearish": bearish_prob,
        "bullishness_percentage": bullishness_score,
    }


def analyze_cashtags(cashtags):
    overall_sentiments = {}

    for cashtag in cashtags:
        search_term = cashtag[1:] if len(cashtag) > 6 else cashtag
        print(f"Analyzing tweets for {search_term}...")

        run_input = {
            "searchTerms": [search_term],
            "maxItems": 50,
            "sort": "Latest",
            "tweetLanguage": "en",
        }

        try:
            run = client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input)

            bullishness_scores = []  # Store rounded bullishness percentages
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                raw_tweet = item.get("text", "").strip()
                tweet_text = preprocess_tweet(raw_tweet)

                if not is_relevant_tweet(tweet_text):
                    continue

                # Calculate bullishness and append rounded value
                sentiment_result = calculate_bullishness(tweet_text)
                rounded_score = round(sentiment_result["bullishness_percentage"], 2)
                bullishness_scores.append(rounded_score)

            if bullishness_scores:
                # Compute final average
                average_bullishness = round(sum(bullishness_scores) / len(bullishness_scores), 2)
                # Use f-string formatting for consistent decimal places
                overall_sentiments[cashtag] = f"{average_bullishness:.2f}% bullish"
            else:
                overall_sentiments[cashtag] = "No tweets found"

        except Exception as e:
            print(f"Error analyzing tweets for {search_term}: {e}")
            overall_sentiments[cashtag] = "Error fetching tweets"

    return overall_sentiments
