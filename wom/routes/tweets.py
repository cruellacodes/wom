import asyncio
import logging
from fastapi import APIRouter, Query, HTTPException, Request # type: ignore
from services.tweet_service import fetch_stored_tweets

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
async def queue_search_on_demand(token_symbol: str, request: Request):
    queue = request.app.state.search_queue
    future = asyncio.get_event_loop().create_future()

    # Queue this search
    await queue.put((token_symbol.lower(), future))

    try:
        result = await asyncio.wait_for(future, timeout=30)
        return result
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Search timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))