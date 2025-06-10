import asyncio
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, List, Optional
import aiohttp
import time
import os
from functools import lru_cache

from services.tweet_service import TweetService, ServiceError, TextCleaner, DateTimeHandler

# Global state for managing in-progress searches
in_progress: dict[str, asyncio.Future] = {}

# Global tweet service instance for WOM calculations and processing logic
_search_tweet_service: Optional[TweetService] = None

@lru_cache()
def get_api_config() -> Dict[str, Any]:
    """Get API configuration from environment variables"""
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    rapidapi_host = os.getenv("RAPIDAPI_HOST", "twitter241.p.rapidapi.com")
    api_url = f"https://{rapidapi_host}/v2/search"  # Updated endpoint
    
    if not rapidapi_key:
        raise ValueError("RAPIDAPI_KEY environment variable is required")
    
    return {
        "url": api_url,
        "headers": {
            "X-RapidAPI-Key": rapidapi_key,
            "X-RapidAPI-Host": rapidapi_host
        },
        "timeout": int(os.getenv("API_TIMEOUT", "15")),
        "max_retries": int(os.getenv("API_MAX_RETRIES", "2"))
    }

# Cache for API results
search_cache = {}
CACHE_DURATION = 300  # 5 minutes

async def get_search_tweet_service() -> TweetService:
    """Get or create tweet service instance for WOM calculations and processing logic"""
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
    """Handle on-demand search for a token using the actual API endpoint"""
    try:
        # Check cache first
        cache_key = f"{token_symbol}_{int(time.time() // CACHE_DURATION)}"
        if cache_key in search_cache:
            logging.info(f"[cache] Returning cached result for {token_symbol}")
            return search_cache[cache_key]
        
        # Fetch tweets from API endpoint
        raw_tweets = await _fetch_tweets_last_24h(token_symbol)
        
        if not raw_tweets:
            logging.info(f"[search] No tweets found for {token_symbol}")
            result = {
                "token_symbol": token_symbol,
                "tweets": [],
                "wom_score": 0.0,
                "tweet_count": 0,
                "message": "No recent tweets found"
            }
            search_cache[cache_key] = result
            return result

        # Process tweets using TweetService processing logic
        processed_tweets = await _process_tweets(raw_tweets, token_symbol)
        
        if not processed_tweets:
            logging.info(f"[search] No tweets passed processing for {token_symbol}")
            result = {
                "token_symbol": token_symbol,
                "tweets": [],
                "wom_score": 0.0,
                "tweet_count": 0,
                "message": "No tweets passed filtering"
            }
            search_cache[cache_key] = result
            return result

        # Convert to response format and calculate WOM score
        response_tweets = []
        tweet_records = []
        
        for tweet in processed_tweets:
            response_tweet = {
                "tweet_id": tweet["tweet_id"],
                "text": tweet["text"],
                "user_name": tweet["user_name"],
                "followers_count": tweet["followers_count"],
                "profile_pic": tweet["profile_pic"],
                "created_at": tweet["created_at"],
                "wom_score": tweet["wom_score"],
                "tweet_url": tweet["tweet_url"]
            }
            response_tweets.append(response_tweet)
            
            tweet_record = {
                "created_at": DateTimeHandler.parse_twitter_time(tweet["created_at"]),
                "wom_score": tweet["wom_score"],
                "followers_count": tweet["followers_count"],
                "user_name": tweet["user_name"]
            }
            tweet_records.append(tweet_record)

        # Use the tweet service's WOM calculator
        tweet_service = await get_search_tweet_service()
        final_wom_score = tweet_service.wom_calculator.calculate_final_wom_score(tweet_records)
        
        result = {
            "token_symbol": token_symbol,
            "tweets": response_tweets,
            "wom_score": final_wom_score,
            "tweet_count": len(processed_tweets),
            "search_timestamp": DateTimeHandler.now().isoformat()
        }
        
        # Cache the result
        search_cache[cache_key] = result
        
        logging.info(f"[search] Found {len(processed_tweets)} tweets for {token_symbol}, WOM score: {final_wom_score}")
        return result

    except Exception as e:
        logging.error(f"[search] Unexpected error for {token_symbol}: {e}")
        raise Exception(f"Search failed: {e}")

async def _fetch_tweets_last_24h(token_symbol: str, retries: int = 0) -> List[Dict]:
    """Fetch tweets from the last 24 hours using the actual API structure"""
    config = get_api_config()
    
    async with aiohttp.ClientSession() as session:
        try:
            # Remove $ prefix to get clean token for API query construction
            clean_token = token_symbol.replace("$", "").strip()
            
            # Use $ prefix for the search query
            query = f"${clean_token}"
            
            # Calculate 24 hours ago for filtering
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            params = {
                "query": query,
                "count": 100,  # Fetch more to ensure we get enough recent ones
                "result_type": "recent"
            }
            
            async with session.get(
                config["url"],
                headers=config["headers"],
                params=params,
                timeout=aiohttp.ClientTimeout(total=config["timeout"])
            ) as response:
                
                if response.status == 429 and retries < config["max_retries"]:
                    # Rate limited, wait and retry
                    wait_time = 2 ** retries
                    logging.warning(f"[api] Rate limited, waiting {wait_time}s before retry")
                    await asyncio.sleep(wait_time)
                    return await _fetch_tweets_last_24h(token_symbol, retries + 1)
                
                # Check content type and handle empty responses
                content_type = response.headers.get('content-type', '').lower()
                if 'application/json' not in content_type:
                    logging.error(f"[api] Unexpected content type for {token_symbol}: {content_type}")
                    return []
                
                response_text = await response.text()
                if not response_text or response_text.strip() == '':
                    logging.warning(f"[api] Empty response body for {token_symbol}")
                    return []
                
                try:
                    data = await response.json()
                except Exception as json_error:
                    logging.error(f"[api] JSON parsing failed for {token_symbol}: {json_error}")
                    return []
                
                if data is None or not isinstance(data, dict):
                    logging.warning(f"[api] Invalid response structure for {token_symbol}")
                    return []
                
                response.raise_for_status()
                
                # Parse the actual API response structure
                tweets = []
                
                # Navigate through the actual response structure from your example
                result = data.get("result", {})
                timeline = result.get("timeline", {})
                instructions = timeline.get("instructions", [])
                
                for instruction in instructions:
                    if instruction.get("type") == "TimelineAddEntries":
                        entries = instruction.get("entries", [])
                        
                        for entry in entries:
                            # Skip cursor entries
                            if entry.get("entryId", "").startswith("cursor-"):
                                continue
                                
                            content = entry.get("content", {})
                            if content.get("entryType") != "TimelineTimelineItem":
                                continue
                                
                            item_content = content.get("itemContent", {})
                            if item_content.get("itemType") != "TimelineTweet":
                                continue
                            
                            tweet_results = item_content.get("tweet_results", {})
                            tweet_result = tweet_results.get("result", {})
                            
                            if tweet_result.get("__typename") != "Tweet":
                                continue
                            
                            # Extract tweet data
                            legacy = tweet_result.get("legacy", {})
                            core = tweet_result.get("core", {})
                            user_results = core.get("user_results", {})
                            user_result = user_results.get("result", {})
                            user_legacy = user_result.get("legacy", {})
                            
                            # Parse created_at and filter to last 24 hours
                            created_at_str = legacy.get("created_at")
                            if not created_at_str:
                                continue
                                
                            # Parse Twitter's date format: "Tue Jun 10 04:32:20 +0000 2025"
                            try:
                                created_at = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
                                # Convert to naive datetime for comparison
                                created_at_naive = created_at.replace(tzinfo=None)
                                
                                if created_at_naive < cutoff_time:
                                    continue  # Skip tweets older than 24 hours
                                    
                            except ValueError as e:
                                logging.warning(f"[api] Could not parse date {created_at_str}: {e}")
                                continue
                            
                            # Extract tweet text
                            text = legacy.get("full_text", "")
                            if not text:
                                continue
                            
                            # Create tweet object in expected format
                            tweet = {
                                "tweet_id": legacy.get("id_str"),
                                "text": text,
                                "created_at": created_at_str,
                                "user": {
                                    "screen_name": user_legacy.get("screen_name", ""),
                                    "followers_count": user_legacy.get("followers_count", 0),
                                    "profile_image_url": user_legacy.get("profile_image_url_https", "")
                                }
                            }
                            tweets.append(tweet)
                
                logging.info(f"[api] Fetched {len(tweets)} tweets from last 24h for {token_symbol}")
                return tweets
                
        except aiohttp.ClientError as e:
            if retries < config["max_retries"]:
                wait_time = 2 ** retries
                logging.warning(f"[api] Request failed, retrying in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)
                return await _fetch_tweets_last_24h(token_symbol, retries + 1)
            else:
                logging.error(f"[api] API request failed after {retries} retries: {e}")
                return []
        except Exception as e:
            logging.error(f"[api] Unexpected error for {token_symbol}: {e}")
            return []

async def _process_tweets(raw_tweets: List[Dict], token_symbol: str) -> List[Dict]:
    """Process tweets using the same logic as TweetService"""
    if not raw_tweets:
        return []
    
    # Get tweet service instance to use its processor
    tweet_service = await get_search_tweet_service()
    
    # Convert raw tweets to RawTweet objects (same format as TweetService expects)
    from services.tweet_service import UserInfo, RawTweet
    raw_tweet_objects = []
    
    for tweet in raw_tweets:
        try:
            # Basic validation
            if not tweet.get("text") or not tweet.get("tweet_id"):
                continue
            
            # Parse creation time
            created_at = DateTimeHandler.parse_twitter_time(tweet.get("created_at"))
            if not created_at:
                continue
            
            # Extract user info
            user_info = tweet.get("user", {})
            user = UserInfo(
                screen_name=user_info.get("screen_name", ""),
                followers_count=user_info.get("followers_count", 0),
                avatar=user_info.get("profile_image_url", "")
            )
            
            raw_tweet = RawTweet(
                tweet_id=tweet["tweet_id"],
                text=tweet["text"],
                created_at=created_at,
                user_info=user
            )
            raw_tweet_objects.append(raw_tweet)
            
        except Exception as e:
            logging.warning(f"[processing] Error converting tweet {tweet.get('tweet_id')}: {e}")
            continue
    
    # Use TweetService's processor to process tweets (includes filtering, sentiment analysis, etc.)
    processed_tweets = await tweet_service.tweet_processor.process_tweets(raw_tweet_objects, token_symbol)
    
    # Convert ProcessedTweet objects back to dict format for compatibility
    processed_dicts = []
    for tweet in processed_tweets:
        tweet_dict = {
            "tweet_id": tweet.tweet_id,
            "text": tweet.text,
            "user_name": tweet.user_name,
            "followers_count": tweet.followers_count,
            "profile_pic": tweet.profile_pic,
            "created_at": tweet.created_at.isoformat(),
            "wom_score": tweet.wom_score,
            "tweet_url": tweet.tweet_url
        }
        processed_dicts.append(tweet_dict)
    
    logging.info(f"[processing] Processed {len(processed_dicts)} tweets using TweetService logic")
    return processed_dicts

# Add debugging function for testing
async def debug_api_response(token_symbol: str = "ibrl") -> dict:
    """Debug function to inspect what the API actually returns"""
    config = get_api_config()
    
    async with aiohttp.ClientSession() as session:
        try:
            clean_token = token_symbol.replace("$", "").strip()
            query = f"${clean_token}"
            
            params = {
                "query": query,
                "count": 5,  # Small count for testing
                "result_type": "recent"
            }
            
            logging.info(f"[debug] Making request to: {config['url']}")
            logging.info(f"[debug] Headers: {config['headers']}")
            logging.info(f"[debug] Params: {params}")
            
            async with session.get(
                config["url"],
                headers=config["headers"],
                params=params,
                timeout=aiohttp.ClientTimeout(total=config["timeout"])
            ) as response:
                
                logging.info(f"[debug] Response status: {response.status}")
                logging.info(f"[debug] Response headers: {dict(response.headers)}")
                
                # Get raw response text first
                response_text = await response.text()
                logging.info(f"[debug] Raw response length: {len(response_text)}")
                logging.info(f"[debug] Raw response preview: {response_text[:1000]}")
                
                # Try to parse as JSON
                try:
                    if response_text:
                        import json
                        data = json.loads(response_text)
                        logging.info(f"[debug] Parsed JSON type: {type(data)}")
                        if isinstance(data, dict):
                            logging.info(f"[debug] JSON keys: {list(data.keys())}")
                            
                        return {
                            "status": "success",
                            "response_length": len(response_text),
                            "data_type": str(type(data)),
                            "data_keys": list(data.keys()) if isinstance(data, dict) else None,
                            "sample_data": data
                        }
                    else:
                        return {
                            "status": "empty_response",
                            "response_length": 0,
                            "message": "API returned empty response"
                        }
                except json.JSONDecodeError as e:
                    return {
                        "status": "json_error",
                        "error": str(e),
                        "response_preview": response_text[:500]
                    }
                
        except Exception as e:
            logging.error(f"[debug] Error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

# Keep all other functions the same as they were working
async def handle_bulk_search(token_symbols: List[str]) -> Dict[str, Any]:
    """Handle bulk search for multiple tokens"""
    try:
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
                        "token_symbol": token,
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
            if isinstance(result, Exception):
                results[token] = {
                    "token_symbol": token,
                    "tweets": [],
                    "wom_score": 0.0,
                    "tweet_count": 0,
                    "error": str(result)
                }
            else:
                results[token] = result
        
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
    """Clear the in-progress cache and search cache"""
    global in_progress, search_cache
    cleared_count = len(in_progress)
    cleared_cache = len(search_cache)
    
    # Cancel any pending futures
    for future in in_progress.values():
        if not future.done():
            future.cancel()
    
    in_progress.clear()
    search_cache.clear()
    
    logging.info(f"[search_cache] Cleared {cleared_count} in-progress searches and {cleared_cache} cached results")
    return {"cleared_searches": cleared_count, "cleared_cache_entries": cleared_cache}

# === Backwards Compatibility Functions ===
async def fetch_last_Xh_tweets(token_symbol: str, hours: int = 24):
    """Backwards compatibility - now uses new API"""
    try:
        raw_tweets = await _fetch_tweets_last_24h(token_symbol)
        return raw_tweets
    except Exception as e:
        logging.error(f"[compat] fetch_last_Xh_tweets failed for {token_symbol}: {e}")
        return []

async def preprocess_tweets(raw_tweets, token_symbol):
    """Backwards compatibility - now uses new processing logic"""  
    try:
        processed = await _process_tweets(raw_tweets, token_symbol)
        return {token_symbol: processed}
    except Exception as e:
        logging.error(f"[compat] preprocess_tweets failed for {token_symbol}: {e}")
        return {token_symbol: []}

async def get_sentiment(tweets_by_token):
    """Backwards compatibility - sentiment already calculated"""
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