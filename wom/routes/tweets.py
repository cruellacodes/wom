from fastapi import APIRouter, Query, HTTPException
from services.tweet_service import fetch_stored_tweets, fetch_tweet_volume_last_6h, fetch_and_analyze
import logging

tweets_router = APIRouter()

@tweets_router.get("/stored-tweets/")
async def get_stored_tweets_endpoint(token: str = Query(...)):
    try:
        tweets = await fetch_stored_tweets(token)
        if not tweets:
            return {"message": f"No stored tweets found for {token}"}
        return {"token": token, "tweets": tweets}
    except Exception as e:
        logging.error(f"Error fetching stored tweets for {token}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@tweets_router.get("/tweet-volume/")
async def get_tweet_volume_endpoint(token: str = Query(...)):
    try:
        tweet_volume = await fetch_tweet_volume_last_6h(token)
        return {"token": token, "tweet_volume": tweet_volume}
    except Exception as e:
        logging.error(f"Error fetching tweet volume for {token}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@tweets_router.get("/tweets/{token_symbol}")
async def get_tweets(token_symbol: str):
    try:
        tweets_data = await fetch_and_analyze(token_symbol)
        return {
            "token": token_symbol,
            "tweets": tweets_data.get("tweets", []),
            "wom_score": tweets_data.get("wom_score", 1)
        }
    except Exception as e:
        logging.error(f"Error fetching tweets for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
