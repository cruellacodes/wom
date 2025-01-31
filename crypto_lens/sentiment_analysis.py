import sqlite3
from datetime import datetime, timedelta


DB_FILE = "tweets.db"


def calculate_bullishness_from_db(token_symbol):
    """
    Calculate sentiment and recency for a token based on its tweets in the database.
    Args:
        token_symbol (str): Token symbol (e.g., "$LLM").
    Returns:
        dict: Sentiment score out of 100 and recency flag.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)

    cursor.execute("""
        SELECT sentiment_score, created_at FROM tweets WHERE token_symbol = ?
    """, (token_symbol,))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return {"sentiment_score": 0, "recent_activity": False}  # No tweets found

    sentiment_scores = []
    has_recent_tweets = False

    for sentiment_score, created_at in rows:
        tweet_time = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
        sentiment_scores.append(sentiment_score)
        if tweet_time > one_hour_ago:
            has_recent_tweets = True

    avg_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0
    sentiment_out_of_100 = round(avg_sentiment * 100, 2)

    return {"sentiment_score": sentiment_out_of_100, "recent_activity": has_recent_tweets}


def analyze_sentiments_for_tokens(cashtags):
    """
    Analyze sentiment and recency for multiple tokens.
    Args:
        cashtags (list): List of token symbols (e.g., ["$LLM", "$GRIND"]).
    Returns:
        list: List of token sentiment analysis results.
    """
    results = []
    for cashtag in cashtags:
        analysis = calculate_bullishness_from_db(cashtag)
        results.append({
            "Cashtag": cashtag,
            "Sentiment_Score": analysis["sentiment_score"],
            "Recent_Activity": "Yes" if analysis["recent_activity"] else "No",
        })
    return results
