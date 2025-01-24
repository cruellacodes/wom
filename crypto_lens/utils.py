import re

def preprocess_tweet(tweet_text):
    """Clean and preprocess the tweet text."""
    # Remove URLs
    tweet_text = re.sub(r"http\S+|www\S+|https\S+", "", tweet_text, flags=re.MULTILINE)
    # Remove mentions and hashtags
    tweet_text = re.sub(r"@\w+|#\w+", "", tweet_text)
    # Remove special characters and numbers
    tweet_text = re.sub(r"[^A-Za-z\s]", "", tweet_text)
    # Remove extra whitespace
    tweet_text = tweet_text.strip()
    return tweet_text


def is_relevant_tweet(tweet_text):
    """Check if the tweet contains meaningful content."""
    words = tweet_text.split()
    if len(words) < 3:  # Ignore tweets with fewer than 3 words
        return False
    if all(word.startswith("#") or word.startswith("http") for word in words):  # Ignore tweets with only hashtags or links
        return False
    return True

def weighted_sentiment_score(sentiment_scores, engagements):
    """Calculate the weighted average sentiment score."""
    total_weight = sum(engagements)
    if total_weight == 0:
        return 0
    weighted_score = sum(score * weight for score, weight in zip(sentiment_scores, engagements))
    return weighted_score / total_weight

