import asyncio
import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Query, HTTPException, Request, Depends
from services.search_service import get_or_create_token_future
# from services.volume_service import get_or_create_volume_future
from services.tweet_service import TweetService, ServiceError, TextCleaner

tweets_router = APIRouter()

# Dependency to get tweet service from app state
async def get_tweet_service(request: Request) -> Optional[TweetService]:
    """Get tweet service from app state"""
    return getattr(request.app.state, 'tweet_service', None)

@tweets_router.get("/stored-tweets/")
async def get_stored_tweets_endpoint(
    token_symbol: str = Query(..., description="Token symbol to fetch tweets for"),
    hours: int = Query(48, description="Hours of tweets to fetch (default: 48)", ge=1, le=168),
    tweet_service: Optional[TweetService] = Depends(get_tweet_service)
):
    """Get stored tweets for a token symbol from the database"""
    try:
        if not tweet_service:
            raise HTTPException(status_code=503, detail="Tweet service unavailable")
        
        # Normalize token symbol
        normalized_token = TextCleaner.normalize_token(token_symbol)
        
        # Get tweets from database
        tweets = await tweet_service.db_manager.get_recent_tweets(normalized_token, hours)
        
        if not tweets:
            return {
                "message": f"No stored tweets found for {token_symbol}",
                "token_symbol": normalized_token,
                "tweets": [],
                "count": 0
            }
        
        # Convert database records to API response format
        formatted_tweets = []
        for tweet in tweets:
            formatted_tweet = {
                "tweet_id": tweet["tweet_id"],
                "text": tweet["text"],
                "user_name": tweet["user_name"],
                "followers_count": tweet["followers_count"],
                "profile_pic": tweet["profile_pic"],
                "created_at": tweet["created_at"].isoformat() if tweet["created_at"] else None,
                "wom_score": tweet["wom_score"],
                "tweet_url": tweet["tweet_url"]
            }
            formatted_tweets.append(formatted_tweet)
        
        return {
            "token_symbol": normalized_token,
            "tweets": formatted_tweets,
            "count": len(formatted_tweets),
            "hours_range": hours
        }
        
    except ServiceError as e:
        logging.error(f"Service error fetching stored tweets for {token_symbol}: {e}")
        raise HTTPException(status_code=503, detail="Tweet service error")
    except Exception as e:
        logging.error(f"Error fetching stored tweets for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@tweets_router.get("/tweets/{token_symbol}")
async def queue_search_on_demand(token_symbol: str, request: Request):
    """Queue a search for tweets on-demand via search service"""
    try:
        token_symbol = token_symbol.lower()
        queue = request.app.state.search_queue

        if not queue:
            raise HTTPException(status_code=503, detail="Search queue unavailable")

        future = get_or_create_token_future(token_symbol, queue)

        try:
            result = await asyncio.wait_for(future, timeout=60)
            return result
        except asyncio.TimeoutError:
            raise HTTPException(status_code=504, detail="Search timed out")
            
    except Exception as e:
        logging.error(f"Error in on-demand search for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# @tweets_router.get("/volume/{token_symbol}")
# async def queue_volume_count(token_symbol: str, request: Request):
#     """Queue a volume count request for a token"""
#     try:
#         # Check if volume queue exists
#         if not hasattr(request.app.state, 'volume_queue'):
#             raise HTTPException(status_code=503, detail="Volume queue not configured")
        
#         queue = request.app.state.volume_queue
#         future = get_or_create_volume_future(token_symbol.lower(), queue)

#         try:
#             result = await asyncio.wait_for(future, timeout=30)
#             return result
#         except asyncio.TimeoutError:
#             raise HTTPException(status_code=504, detail="Volume search timed out")
            
#     except Exception as e:
#         logging.error(f"Error in volume search for {token_symbol}: {e}")
#         raise HTTPException(status_code=500, detail=f"Volume search failed: {e}")

@tweets_router.post("/tweets/{token_symbol}/refresh")
async def refresh_tweets_for_token(
    token_symbol: str,
    tweet_service: Optional[TweetService] = Depends(get_tweet_service)
):
    """Manually trigger tweet fetching and processing for a specific token"""
    try:
        if not tweet_service:
            raise HTTPException(status_code=503, detail="Tweet service unavailable")
        
        normalized_token = TextCleaner.normalize_token(token_symbol)
        
        # Check if token is active
        active_tokens = await tweet_service.db_manager.get_active_tokens()
        if normalized_token not in active_tokens:
            raise HTTPException(status_code=404, detail=f"Token {token_symbol} is not active")
        
        # Fetch and store tweets for this token
        success = await tweet_service.fetch_and_store_tweets_for_token(normalized_token)
        
        if success:
            # Recalculate WOM score for this token
            await tweet_service._recalculate_token_wom_score(normalized_token)
            
            return {
                "status": "success",
                "message": f"Successfully refreshed tweets for {token_symbol}",
                "token_symbol": normalized_token
            }
        else:
            return {
                "status": "no_new_data",
                "message": f"No new tweets found for {token_symbol}",
                "token_symbol": normalized_token
            }
            
    except ServiceError as e:
        logging.error(f"Service error refreshing tweets for {token_symbol}: {e}")
        raise HTTPException(status_code=503, detail="Tweet service error")
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        logging.error(f"Error refreshing tweets for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@tweets_router.get("/tweets/{token_symbol}/stats")
async def get_tweet_stats(
    token_symbol: str,
    hours: int = Query(48, description="Hours to analyze (default: 48)", ge=1, le=168),
    tweet_service: Optional[TweetService] = Depends(get_tweet_service)
):
    """Get statistical information about tweets for a token"""
    try:
        if not tweet_service:
            raise HTTPException(status_code=503, detail="Tweet service unavailable")
        
        normalized_token = TextCleaner.normalize_token(token_symbol)
        
        # Get recent tweets
        tweets = await tweet_service.db_manager.get_recent_tweets(normalized_token, hours)
        
        if not tweets:
            return {
                "token_symbol": normalized_token,
                "stats": {
                    "total_tweets": 0,
                    "avg_wom_score": 0,
                    "avg_followers": 0,
                    "hours_analyzed": hours
                }
            }
        
        # Calculate statistics
        total_tweets = len(tweets)
        wom_scores = [t["wom_score"] for t in tweets if t["wom_score"] is not None]
        followers_counts = [t["followers_count"] for t in tweets if t["followers_count"] is not None]
        
        avg_wom_score = sum(wom_scores) / len(wom_scores) if wom_scores else 0
        avg_followers = sum(followers_counts) / len(followers_counts) if followers_counts else 0
        
        # Calculate current WOM score using the calculator
        current_wom = tweet_service.wom_calculator.calculate_final_wom_score(tweets)
        
        # Get unique users
        unique_users = len(set(t["user_name"] for t in tweets if t["user_name"]))
        
        # Get time range
        if tweets:
            sorted_tweets = sorted(tweets, key=lambda x: x["created_at"])
            oldest_tweet = sorted_tweets[0]["created_at"]
            newest_tweet = sorted_tweets[-1]["created_at"]
        else:
            oldest_tweet = newest_tweet = None
        
        return {
            "token_symbol": normalized_token,
            "stats": {
                "total_tweets": total_tweets,
                "unique_users": unique_users,
                "avg_wom_score": round(avg_wom_score, 2),
                "current_wom_score": current_wom,
                "avg_followers": int(avg_followers),
                "hours_analyzed": hours,
                "oldest_tweet": oldest_tweet.isoformat() if oldest_tweet else None,
                "newest_tweet": newest_tweet.isoformat() if newest_tweet else None
            }
        }
        
    except ServiceError as e:
        logging.error(f"Service error getting stats for {token_symbol}: {e}")
        raise HTTPException(status_code=503, detail="Tweet service error")
    except Exception as e:
        logging.error(f"Error getting stats for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@tweets_router.get("/health")
async def tweets_health_check(tweet_service: Optional[TweetService] = Depends(get_tweet_service)):
    """Health check for tweet-related services"""
    return {
        "tweet_service": "available" if tweet_service else "unavailable",
        "sentiment_analyzer": "initialized" if tweet_service and getattr(tweet_service.sentiment_analyzer, '_initialized', False) else "not_initialized"
    }