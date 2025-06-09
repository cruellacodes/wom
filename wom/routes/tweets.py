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
        token_symbol = TextCleaner.normalize_token(token_symbol)
        
        # Get tweets from database
        tweets = await tweet_service.db_manager.get_recent_tweets(token_symbol, hours)
        
        if not tweets:
            return {
                "message": f"No stored tweets found for {token_symbol}",
                "token_symbol": token_symbol,
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
            "token_symbol": token_symbol,
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
        
        if not token_symbol.startswith('$'):
            token_symbol = f'${token_symbol}'
        
        # Check if token is active
        active_tokens = await tweet_service.db_manager.get_active_tokens()
        if token_symbol not in active_tokens:
            raise HTTPException(status_code=404, detail=f"Token {token_symbol} is not active")
        
        # Fetch and store tweets for this token
        success = await tweet_service.fetch_and_store_tweets_for_token(token_symbol)
        
        if success:
            # Recalculate WOM score for this token
            await tweet_service._recalculate_token_wom_score(token_symbol)
            
            return {
                "status": "success",
                "message": f"Successfully refreshed tweets for {token_symbol}",
                "token_symbol": token_symbol
            }
        else:
            return {
                "status": "no_new_data",
                "message": f"No new tweets found for {token_symbol}",
                "token_symbol": token_symbol
            }
            
    except ServiceError as e:
        logging.error(f"Service error refreshing tweets for {token_symbol}: {e}")
        raise HTTPException(status_code=503, detail="Tweet service error")
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        logging.error(f"Error refreshing tweets for {token_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@tweets_router.post("/tweets/{token_symbol}/run-pipeline")
async def run_tweet_pipeline_for_token(
    token_symbol: str,
    request: Request,
    tweet_service: Optional[TweetService] = Depends(get_tweet_service)
):
    """Run the complete tweet pipeline for a specific token symbol"""
    try:
        if not tweet_service:
            raise HTTPException(status_code=503, detail="Tweet service unavailable")
        
        if not token_symbol.startswith('$'):
            token_symbol = f'${token_symbol}'
        
        # Step 1: Ensure token is active (add if not exists)
        try:
            active_tokens = await tweet_service.db_manager.get_active_tokens()
            if token_symbol not in active_tokens:
                # Add token to active tokens if it doesn't exist
                await tweet_service.db_manager.add_token(token_symbol)
                logging.info(f"Added {token_symbol} to active tokens")
        except Exception as e:
            logging.warning(f"Could not verify/add token {token_symbol}: {e}")
        
        pipeline_results = {
            "token_symbol": token_symbol,
            "steps_completed": [],
            "errors": []
        }
        
        # Step 2: Run tweet pipeline - fetch and store tweets
        try:
            logging.info(f"Running tweet fetch pipeline for {token_symbol}")
            
            # Check if there are pipeline methods available on tweet_service
            if hasattr(tweet_service, 'run_tweet_pipeline'):
                await tweet_service.run_tweet_pipeline()
                pipeline_results["steps_completed"].append("tweet_pipeline")
            else:
                # Fall back to individual token processing
                success = await tweet_service.fetch_and_store_tweets_for_token(token_symbol)
                if success:
                    pipeline_results["steps_completed"].append("tweet_fetch_and_store")
                else:
                    pipeline_results["errors"].append("No new tweets found during fetch")
            
        except Exception as e:
            error_msg = f"Tweet pipeline failed: {str(e)}"
            logging.error(error_msg)
            pipeline_results["errors"].append(error_msg)
        
        # Step 3: Run score pipeline - calculate WOM scores
        try:
            logging.info(f"Running score pipeline for {token_symbol}")
            
            if hasattr(tweet_service, 'run_score_pipeline'):
                await tweet_service.run_score_pipeline()
                pipeline_results["steps_completed"].append("score_pipeline")
            else:
                # Fall back to individual token score calculation
                await tweet_service._recalculate_token_wom_score(token_symbol)
                pipeline_results["steps_completed"].append("wom_score_calculation")
            
        except Exception as e:
            error_msg = f"Score pipeline failed: {str(e)}"
            logging.error(error_msg)
            pipeline_results["errors"].append(error_msg)
        
        # Step 4: Get final stats
        try:
            tweets = await tweet_service.db_manager.get_recent_tweets(token_symbol, 48)
            pipeline_results["final_stats"] = {
                "total_tweets": len(tweets),
                "current_wom_score": tweet_service.wom_calculator.calculate_final_wom_score(tweets) if tweets else 0
            }
        except Exception as e:
            pipeline_results["errors"].append(f"Could not fetch final stats: {str(e)}")
        
        # Determine overall status
        if pipeline_results["steps_completed"]:
            if pipeline_results["errors"]:
                status = "partial_success"
                message = f"Pipeline completed with some errors for {token_symbol}"
            else:
                status = "success"
                message = f"Pipeline completed successfully for {token_symbol}"
        else:
            status = "failed"
            message = f"Pipeline failed for {token_symbol}"
        
        return {
            "status": status,
            "message": message,
            **pipeline_results
        }
        
    except ServiceError as e:
        logging.error(f"Service error running pipeline for {token_symbol}: {e}")
        raise HTTPException(status_code=503, detail="Tweet service error")
    except Exception as e:
        logging.error(f"Error running pipeline for {token_symbol}: {e}")
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

        if not token_symbol.startswith('$'):
            token_symbol = f'${token_symbol}'
        
        # Get recent tweets
        tweets = await tweet_service.db_manager.get_recent_tweets(token_symbol, hours)
        
        if not tweets:
            return {
                "token_symbol": token_symbol,
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
            "token_symbol": token_symbol,
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