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
    """
    Check if the tweet contains meaningful content and applies filters:
    - Exclude tweets with more than 3 combined hashtags and cashtags.
    - Exclude tweets containing the rocket emoji (🚀).
    - Exclude tweets with fewer than 3 words.
    """
    # Check for hashtags and cashtags
    hashtag_count = len(re.findall(r"#\w+", tweet_text))
    cashtag_count = len(re.findall(r"\$\w+", tweet_text))
    
    # Exclude tweets with more than 3 combined hashtags/cashtags
    if hashtag_count + cashtag_count > 3:
        return False

    # Exclude tweets with the rocket emoji
    if "🚀" in tweet_text:
        return False

    # Split tweet into words
    words = tweet_text.split()

    # Ignore tweets with fewer than 3 words
    if len(words) < 3:
        return False

    return True


def weighted_sentiment_score(sentiment_scores, engagements):
    """Calculate the weighted average sentiment score."""
    total_weight = sum(engagements)
    if total_weight == 0:
        return 0
    weighted_score = sum(score * weight for score, weight in zip(sentiment_scores, engagements))
    return weighted_score / total_weight
