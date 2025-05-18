from fastapi import APIRouter, Query, HTTPException # type: ignore
from services.tweet_service import fetch_stored_tweets, fetch_and_analyze, fetch_tweet_volume_buckets
import logging

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
        tweets_data = await fetch_and_analyze(token_symbol)
        return {
            "token_symbol": token_symbol,
            "tweets": tweets_data.get("tweets", []),
            "wom_score": tweets_data.get("wom_score", 1)
        }
    except Exception as e:
        logging.error(f"Error fetching tweets for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")