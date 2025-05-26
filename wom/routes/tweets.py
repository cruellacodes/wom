import asyncio
import logging
from fastapi import APIRouter, Query, HTTPException # type: ignore
from services.tweet_service import fetch_stored_tweets, handle_on_demand_search

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
        result = await asyncio.create_task(handle_on_demand_search(token_symbol))
        return result
    except Exception as e:
        logging.error(f"Error in search-on-demand for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")