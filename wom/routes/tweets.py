from fastapi import APIRouter, Query, HTTPException
from services.tweet_service import fetch_stored_tweets, fetch_tweet_volume_last_6h, fetch_and_analyze, fetch_tweet_volume_buckets
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


@tweets_router.get("/tweet-volume/")
async def get_tweet_volume_endpoint(token_symbol: str = Query(...)):
    try:
        tweet_volume = await fetch_tweet_volume_last_6h(token_symbol)
        return {"token_symbol": token_symbol, "tweet_volume": tweet_volume}
    except Exception as e:
        logging.error(f"Error fetching tweet volume for {token_symbol}: {e}")
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

@tweets_router.get("/tweet-buckets/")
async def get_tweet_buckets_endpoint(token_symbol: str = Query(...)):
    try:
        result = await fetch_tweet_volume_buckets(token_symbol.lower().strip())
        if not result:
            raise HTTPException(status_code=404, detail="No bucket data found for this token.")

        return {"token_symbol": token_symbol, "bucket_data": result}
    except Exception as e:
        logging.error(f"Error fetching tweet bucket data for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")