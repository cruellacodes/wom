import os
import pandas as pd
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
    probs = softmax(outputs.logits.detach().numpy()[0])

    # Probabilities for bullish, neutral, and bearish
    bullish_prob = round(probs[0] * 100, 2)
    bearish_prob = round(probs[2] * 100, 2)
    bullishness_score = round(bullish_prob / (bullish_prob + bearish_prob), 2)
    return bullishness_score


def analyze_cashtags(cashtags):
    """
    Analyze sentiment for a list of cashtags using different actors
    based on the length of the search term.
    """
    data = []

    for cashtag in cashtags:
        # Determine search term (remove $ prefix for querying)
        search_term = cashtag[1:]

        # Select appropriate actor based on search term length
        if len(search_term) <= 6:
            # Use the shorter actor (wHootRXb00ztxCELq)
            run_input = {
                "cashtag": search_term,
                "sentimentAnalysis": None,
                "cookies": [
                    "auth_token=7a0de5bb4260b62eb16615586465d03bedcd4bf9;gt=1883340780888289298;..."
                ],
                "sortBy": "Latest",
                "maxItems": 10,
                "minRetweets": 0,
                "minLikes": 0,
                "minReplies": 0,
                "onlyVerifiedUsers": None,
                "onlyBuleVerifiedUsers": None,
            }
            actor_id = "wHootRXb00ztxCELq"  # Actor for short search terms
        else:
            # Use the longer actor (bQ0LeyXn6BO51yFDY)
            run_input = {
                "hashtag": search_term,
                "sentimentAnalysis": None,
                "cookies": [
                    "auth_token=7a0de5bb4260b62eb16615586465d03bedcd4bf9;gt=1883340780888289298;..."
                ],
                "sortBy": "Latest",
                "maxItems": 10,
                "minRetweets": 0,
                "minLikes": 0,
                "minReplies": 0,
                "onlyVerifiedUsers": None,
                "onlyBuleVerifiedUsers": None,
            }
            actor_id = "bQ0LeyXn6BO51yFDY"  # Actor for long search terms

        try:
            # Run the selected actor with the appropriate input
            run = client.actor(actor_id).call(run_input=run_input)
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
                    continue  # Skip this tweet if follower count is too low

                # Calculate bullishness and store it
                score = calculate_bullishness(tweet_text)
                bullishness_scores.append(score)

                # Append tweet data to the results
                data.append(
                    {
                        "Cashtag": cashtag,
                        "Bullishness": score,
                        "created_at": created_at,  
                        "Tweet_Text": raw_tweet, 
                    }
                )

            # Calculate average bullishness
            if bullishness_scores:
                avg_bullishness = round(sum(bullishness_scores) / len(bullishness_scores), 2)
                data.append({"Cashtag": cashtag, "Bullishness": avg_bullishness})

        except Exception as e:
            print(f"Error analyzing cashtag {cashtag}: {e}")
            data.append({"Cashtag": cashtag, "Bullishness": "Error"})

    # Return the results as a Pandas DataFrame
    return pd.DataFrame(data)
