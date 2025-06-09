import asyncio
from datetime import datetime
import logging
from typing import Dict, Any, List, Optional

from services.tweet_service import TweetService, ServiceError, TextCleaner, DateTimeHandler

# Global state for managing in-progress searches
in_progress: dict[str, asyncio.Future] = {}

# Global tweet service instance for search operations
_search_tweet_service: Optional[TweetService] = None

async def get_search_tweet_service() -> TweetService:
    """Get or create tweet service instance for search operations"""
    global _search_tweet_service
    if _search_tweet_service is None:
        _search_tweet_service = TweetService()
        await _search_tweet_service.initialize()
    return _search_tweet_service

def get_or_create_token_future(token_symbol: str, queue: asyncio.Queue) -> asyncio.Future:
    """Get existing future or create new one for token search"""
    loop = asyncio.get_event_loop()

    if token_symbol in in_progress:
        logging.info(f"[dedupe] Reusing in-progress search for {token_symbol}")
        return in_progress[token_symbol]

    future = loop.create_future()
    in_progress[token_symbol] = future
    
    try:
        queue.put_nowait((token_symbol, future))
    except asyncio.QueueFull:
        logging.warning(f"[search_queue] Queue full, rejecting search for {token_symbol}")
        future.set_exception(Exception("Search queue is full"))
        in_progress.pop(token_symbol, None)
    
    return future

async def process_search_queue(queue: asyncio.Queue):
    """Process search requests from the queue"""
    if queue.empty():
        return

    try:
        token_symbol, future = await asyncio.wait_for(queue.get(), timeout=1.0)
        logging.info(f"[search_queue] Processing search for {token_symbol}")

        try:
            result = await handle_on_demand_search(token_symbol)
            if not future.done():
                future.set_result(result)
        except Exception as e:
            logging.error(f"[search_queue] Failed to process {token_symbol}: {e}")
            if not future.done():
                future.set_exception(e)
        finally:
            # Always clean up the in_progress entry
            in_progress.pop(token_symbol, None)
            
    except asyncio.TimeoutError:
        # No items in queue, this is normal
        pass
    except Exception as e:
        logging.error(f"[search_queue] Error processing queue: {e}")

async def handle_on_demand_search(token_symbol: str) -> Dict[str, Any]:
    """Handle on-demand search for a token using the tweet service"""
    try:
        # Get tweet service instance
        tweet_service = await get_search_tweet_service()
        
        # Normalize token symbol
        normalized_token = TextCleaner.normalize_token(token_symbol)
        
        # Use the tweet service's existing method to fetch all recent tweets
        raw_tweets = await tweet_service._fetch_all_recent_tweets(normalized_token)
        
        if not raw_tweets:
            logging.info(f"[search] No tweets found for {token_symbol}")
            return {
                "token_symbol": normalized_token,
                "tweets": [],
                "wom_score": 0.0,
                "tweet_count": 0,
                "message": "No recent tweets found"
            }

        # Use the tweet service's processor to process tweets
        processed_tweets = await tweet_service.tweet_processor.process_tweets(raw_tweets, normalized_token)
        
        if not processed_tweets:
            logging.info(f"[search] No tweets passed processing for {token_symbol}")
            return {
                "token_symbol": normalized_token,
                "tweets": [],
                "wom_score": 0.0,
                "tweet_count": 0,
                "message": "No tweets passed filtering"
            }

        # Convert processed tweets to response format and WOM calculation format
        response_tweets = []
        tweet_records = []
        
        for tweet in processed_tweets:
            # Format for API response
            response_tweet = {
                "tweet_id": tweet.tweet_id,
                "text": tweet.text,
                "user_name": tweet.user_name,
                "followers_count": tweet.followers_count,
                "profile_pic": tweet.profile_pic,
                "created_at": tweet.created_at.isoformat(),
                "wom_score": tweet.wom_score,
                "tweet_url": tweet.tweet_url
            }
            response_tweets.append(response_tweet)
            
            # Format for WOM score calculation (convert ProcessedTweet to dict)
            tweet_record = {
                "created_at": tweet.created_at,
                "wom_score": tweet.wom_score,
                "followers_count": tweet.followers_count,
                "user_name": tweet.user_name
            }
            tweet_records.append(tweet_record)

        # Use the tweet service's WOM calculator
        final_wom_score = tweet_service.wom_calculator.calculate_final_wom_score(tweet_records)
        
        logging.info(f"[search] Found {len(processed_tweets)} tweets for {token_symbol}, WOM score: {final_wom_score}")
        
        return {
            "token_symbol": normalized_token,
            "tweets": response_tweets,
            "wom_score": final_wom_score,
            "tweet_count": len(processed_tweets),
            "search_timestamp": DateTimeHandler.now().isoformat()
        }

    except ServiceError as e:
        logging.error(f"[search] Service error for {token_symbol}: {e}")
        raise Exception(f"Tweet service error: {e}")
    except Exception as e:
        logging.error(f"[search] Unexpected error for {token_symbol}: {e}")
        raise Exception(f"Search failed: {e}")

async def handle_bulk_search(token_symbols: List[str]) -> Dict[str, Any]:
    """Handle bulk search for multiple tokens"""
    try:
        tweet_service = await get_search_tweet_service()
        results = {}
        
        # Process tokens concurrently but with limited concurrency
        semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent searches
        
        async def search_single_token(token: str):
            async with semaphore:
                try:
                    return await handle_on_demand_search(token)
                except Exception as e:
                    logging.error(f"[bulk_search] Failed for {token}: {e}")
                    return {
                        "token_symbol": TextCleaner.normalize_token(token),
                        "tweets": [],
                        "wom_score": 0.0,
                        "tweet_count": 0,
                        "error": str(e)
                    }
        
        # Execute searches concurrently
        search_results = await asyncio.gather(
            *(search_single_token(token) for token in token_symbols),
            return_exceptions=True
        )
        
        # Process results
        for token, result in zip(token_symbols, search_results):
            normalized_token = TextCleaner.normalize_token(token)
            if isinstance(result, Exception):
                results[normalized_token] = {
                    "token_symbol": normalized_token,
                    "tweets": [],
                    "wom_score": 0.0,
                    "tweet_count": 0,
                    "error": str(result)
                }
            else:
                results[normalized_token] = result
        
        return {
            "bulk_search_results": results,
            "total_tokens": len(token_symbols),
            "successful_searches": len([r for r in search_results if not isinstance(r, Exception)]),
            "search_timestamp": DateTimeHandler.now().isoformat()
        }
        
    except Exception as e:
        logging.error(f"[bulk_search] Failed: {e}")
        raise Exception(f"Bulk search failed: {e}")

async def get_search_queue_status(queue: asyncio.Queue) -> Dict[str, Any]:
    """Get status of the search queue"""
    return {
        "queue_size": queue.qsize(),
        "in_progress_searches": len(in_progress),
        "in_progress_tokens": list(in_progress.keys()),
        "queue_maxsize": queue.maxsize,
        "is_queue_full": queue.full()
    }

async def clear_search_cache():
    """Clear the in-progress cache (useful for debugging/maintenance)"""
    global in_progress
    cleared_count = len(in_progress)
    
    # Cancel any pending futures
    for future in in_progress.values():
        if not future.done():
            future.cancel()
    
    in_progress.clear()
    logging.info(f"[search_cache] Cleared {cleared_count} in-progress searches")
    return {"cleared_searches": cleared_count}

# === Backwards Compatibility Functions ===
# These just call the tweet service methods directly

async def fetch_last_Xh_tweets(token_symbol: str, hours: int = 24):
    """Backwards compatibility - calls tweet service directly"""
    from services.tweet_service import fetch_tweets_from_rapidapi
    return await fetch_tweets_from_rapidapi(token_symbol)

async def preprocess_tweets(raw_tweets, token_symbol):
    """Backwards compatibility - calls tweet service directly"""  
    tweet_service = await get_search_tweet_service()
    
    # Convert old format raw tweets to new RawTweet objects
    from services.tweet_service import UserInfo, RawTweet
    new_format_tweets = []
    
    for tweet in raw_tweets:
        if tweet.get("type") != "tweet":
            continue
            
        created_at = DateTimeHandler.parse_twitter_time(tweet.get("created_at"))
        if not created_at:
            continue
            
        user_info = tweet.get("user_info", {})
        user = UserInfo(
            screen_name=user_info.get("screen_name", ""),
            followers_count=user_info.get("followers_count", 0),
            avatar=user_info.get("avatar", "")
        )
        
        raw_tweet = RawTweet(
            tweet_id=tweet.get("tweet_id"),
            text=tweet.get("text"),
            created_at=created_at,
            user_info=user
        )
        new_format_tweets.append(raw_tweet)
    
    # Use tweet service processor
    processed = await tweet_service.tweet_processor.process_tweets(new_format_tweets, token_symbol)
    
    # Convert back to old format
    old_format_processed = []
    for tweet in processed:
        old_tweet = {
            "tweet_id": tweet.tweet_id,
            "text": tweet.text,
            "user_name": tweet.user_name,
            "followers_count": tweet.followers_count,
            "profile_pic": tweet.profile_pic,
            "created_at": tweet.created_at.isoformat(),
            "wom_score": tweet.wom_score,
            "tweet_url": tweet.tweet_url
        }
        old_format_processed.append(old_tweet)
    
    return {token_symbol: old_format_processed}

async def get_sentiment(tweets_by_token):
    """Backwards compatibility - sentiment already calculated by tweet processor"""
    results = {}
    for token, tweets in tweets_by_token.items():
        if tweets:
            avg_score = sum(t.get("wom_score", 0) for t in tweets) / len(tweets)
            results[token] = {
                "wom_score": avg_score,
                "tweet_count": len(tweets),
                "tweets": tweets
            }
        else:
            results[token] = {
                "wom_score": 0.0,
                "tweet_count": 0,
                "tweets": []
            }
    return results

async def compute_final_wom_score(tweets):
    """Backwards compatibility - calls tweet service WOM calculator"""
    tweet_service = await get_search_tweet_service()
    
    # Convert to format expected by calculator
    tweet_records = []
    for tweet in tweets:
        created_at = tweet.get("created_at")
        if isinstance(created_at, str):
            created_at = DateTimeHandler.parse_twitter_time(created_at)
        
        if created_at:
            record = {
                "created_at": created_at,
                "wom_score": tweet.get("wom_score", 0),
                "followers_count": tweet.get("followers_count", 0),
                "user_name": tweet.get("user_name", "")
            }
            tweet_records.append(record)
    
    return tweet_service.wom_calculator.calculate_final_wom_score(tweet_records)