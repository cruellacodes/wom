import re

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

