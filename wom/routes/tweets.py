from fastapi import APIRouter, Query, HTTPException # type: ignore
from services.tweet_service import compute_final_wom_score, fetch_last_48h_tweets, fetch_stored_tweets, get_sentiment, preprocess_tweets
import logging
from datetime import datetime, timezone

tweets_router = APIRouter()

@tweets_router.get("/stored-tweets/")
async def get_stored_tweets_endpoint(token_symbol: str = Query(...)):
    try:
        tweets = await fetch_stored_tweets(token_symbol)
        if not tweets:
            return {"message": f"No stored tweets found for {token_symbol}"}
        return {"token_symbol": token_symbol, "tweets": tweets}
    except Exception as e:
        logging.error(f"Error fetching stored tweets for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@tweets_router.get("/tweets/{token_symbol}")
async def get_tweets(token_symbol: str):
    try:
        # 1. Fetch raw tweets (no DB)
        raw_tweets = await fetch_last_48h_tweets(token_symbol)

        # 2. Preprocess 
        processed_dict = await preprocess_tweets(raw_tweets, token_symbol)
        tweets = processed_dict.get(token_symbol, [])

        if not tweets:
            return {
                "token_symbol": token_symbol,
                "tweets": [],
                "wom_score": 0.0
            }

        # 3. Sentiment
        sentiment_result = await get_sentiment({token_symbol: tweets})
        scored = sentiment_result.get(token_symbol, {})

        # 4. Final WOM score 
        final_score = compute_final_wom_score([
            {
                "created_at": t["created_at"],
                "wom_score": t["wom_score"]
            } for t in scored.get("tweets", [])
        ])

        return {
            "token_symbol": token_symbol,
            "tweets": scored.get("tweets", []),
            "wom_score": final_score
        }

    except Exception as e:
        logging.error(f"Error fetching tweets for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")