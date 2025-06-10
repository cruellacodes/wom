import asyncio
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, List, Optional
import httpx
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
    """Get API configuration from environment variables - matches TweetService config"""
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    rapidapi_host = os.getenv("RAPIDAPI_HOST")
    
    if not rapidapi_key or not rapidapi_host:
        raise ValueError("RAPIDAPI_KEY and RAPIDAPI_HOST environment variables are required")
    
    return {
        "url": f"https://{rapidapi_host}/search",  # Same endpoint as TweetService
        "headers": {
            "x-rapidapi-key": rapidapi_key,
            "x-rapidapi-host": rapidapi_host
        },
        "timeout": 15,
        "max_retries": 3
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
        logging.info(f"[search_queue] Queued search for {token_symbol}")
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
                logging.info(f"[search_queue] Successfully completed search for {token_symbol}")
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
    """Handle on-demand search for a token using the same API as TweetService"""
    try:
        # Check cache first
        cache_key = f"{token_symbol}_{int(time.time() // CACHE_DURATION)}"
        if cache_key in search_cache:
            logging.info(f"[cache] Returning cached result for {token_symbol}")
            return search_cache[cache_key]
        
        # Fetch tweets from API endpoint using TweetService logic
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

async def _fetch_tweets_last_24h(token_symbol: str, max_pages: int = 5) -> List[Dict]:
    """Fetch tweets from the last 24 hours using the same logic as TweetService"""
    config = get_api_config()
    
    # Use the same query logic as TweetService
    clean_token = token_symbol.replace("$", "").strip()
    query_prefix = "#" if len(clean_token) > 6 else "$"
    query = f"{query_prefix}{clean_token}"
    
    all_tweets = []
    seen_ids = set()
    seen_cursors = set()
    cursor = None
    pages = 0
    
    # Calculate 24 hours ago for filtering
    cutoff_time = datetime.now() - timedelta(hours=24)
    
    async with httpx.AsyncClient(timeout=config["timeout"]) as client:
        while pages < max_pages:
            try:
                # Detect cursor cycles
                if cursor and cursor in seen_cursors:
                    logging.warning(f"Cursor cycle detected for {token_symbol}, breaking")
                    break
                
                if cursor:
                    seen_cursors.add(cursor)
                
                # Build params - same as TweetService
                params = {
                    "type": "Latest",
                    "count": "20",
                    "query": query
                }
                
                if cursor:
                    params["cursor"] = cursor
                
                logging.info(f"[api] Fetching page {pages + 1} for {token_symbol} with query: {query}")
                
                # Make request with retries
                response = await _make_request_with_retries(client, config, params, token_symbol)
                if not response:
                    break
                
                # Parse response using TweetService logic
                page_tweets, next_cursor = _parse_api_response(response, token_symbol)
                
                if not page_tweets:
                    logging.info(f"No tweets found on page {pages + 1} for {token_symbol}")
                    break
                
                # Filter for recent tweets and avoid duplicates
                recent_tweets = []
                for tweet in page_tweets:
                    if tweet["tweet_id"] in seen_ids:
                        continue
                    
                    # Parse tweet time and filter
                    created_at = DateTimeHandler.parse_twitter_time(tweet["created_at"])
                    if not created_at:
                        continue
                        
                    # Convert to naive datetime for comparison
                    created_at_naive = created_at.replace(tzinfo=None) if created_at.tzinfo else created_at
                    
                    if created_at_naive < cutoff_time:
                        continue
                    
                    seen_ids.add(tweet["tweet_id"])
                    recent_tweets.append(tweet)
                
                all_tweets.extend(recent_tweets)
                pages += 1
                
                logging.info(f"[{token_symbol}] Page {pages}: {len(recent_tweets)} recent tweets (total: {len(all_tweets)})")
                
                if not next_cursor:
                    break
                
                cursor = next_cursor
                
            except Exception as e:
                logging.error(f"Error fetching page {pages + 1} for {token_symbol}: {e}")
                break
    
    logging.info(f"Fetched {len(all_tweets)} recent tweets for {token_symbol} across {pages} pages")
    return all_tweets

async def _make_request_with_retries(client: httpx.AsyncClient, config: Dict, params: Dict, 
                                   token_symbol: str, attempt: int = 0) -> Optional[Dict]:
    """Make API request with retry logic - matches TweetService pattern"""
    try:
        response = await client.get(
            config["url"],
            headers=config["headers"],
            params=params
        )
        
        if response.status_code == 429:
            if attempt < config["max_retries"]:
                delay = 2 ** attempt
                logging.warning(f"Rate limited for {token_symbol}, retrying in {delay}s...")
                await asyncio.sleep(delay)
                return await _make_request_with_retries(client, config, params, token_symbol, attempt + 1)
            else:
                logging.error(f"Rate limit exceeded for {token_symbol} after {config['max_retries']} retries")
                return None
        
        response.raise_for_status()
        return response.json()
        
    except httpx.HTTPStatusError as e:
        logging.error(f"HTTP error for {token_symbol}: {e.response.status_code}")
        if attempt < config["max_retries"]:
            delay = 2 ** attempt
            await asyncio.sleep(delay)
            return await _make_request_with_retries(client, config, params, token_symbol, attempt + 1)
        return None
        
    except httpx.RequestError as e:
        logging.warning(f"Request error for {token_symbol}: {e}")
        if attempt < config["max_retries"]:
            delay = 2 ** attempt
            await asyncio.sleep(delay)
            return await _make_request_with_retries(client, config, params, token_symbol, attempt + 1)
        return None
        
    except Exception as e:
        logging.error(f"Unexpected error for {token_symbol}: {e}")
        return None

def _parse_api_response(data: Dict[str, Any], token_symbol: str) -> tuple[List[Dict], Optional[str]]:
    """Parse API response using the exact same logic as TweetService"""
    try:
        instructions = data.get("result", {}).get("timeline", {}).get("instructions", [])
        entries = []
        
        for instr in instructions:
            if instr.get("type") == "TimelineAddEntries":
                entries.extend(instr.get("entries", []))
        
        tweets = []
        next_cursor = None
        
        for entry in entries:
            content = entry.get("content", {})
            
            # Extract cursor
            if (content.get("entryType") == "TimelineTimelineCursor" and 
                content.get("cursorType") == "Bottom"):
                next_cursor = content.get("value")
                continue
            
            # Extract tweet data
            item = content.get("itemContent", {})
            tweet_result = item.get("tweet_results", {}).get("result", {})
            legacy = tweet_result.get("legacy", {})
            user = (tweet_result.get("core", {})
                   .get("user_results", {})
                   .get("result", {})
                   .get("legacy", {}))
            
            if not legacy or not user:
                continue
            
            # Validate required fields
            tweet_id = legacy.get("id_str")
            text = legacy.get("full_text")
            created_at = legacy.get("created_at")
            screen_name = user.get("screen_name")
            
            if not all([tweet_id, text, created_at, screen_name]):
                continue
            
            # Create tweet object in expected format
            tweet = {
                "tweet_id": tweet_id,
                "text": text,
                "created_at": created_at,
                "user": {
                    "screen_name": screen_name,
                    "followers_count": user.get("followers_count", 0),
                    "profile_image_url": user.get("profile_image_url_https", "")
                }
            }
            tweets.append(tweet)
        
        logging.info(f"Parsed {len(tweets)} tweets for {token_symbol}")
        return tweets, next_cursor
        
    except Exception as e:
        logging.error(f"Failed to parse API response for {token_symbol}: {e}")
        return [], None

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
async def debug_api_response(token_symbol: str = "sse") -> dict:
    """Debug function to inspect what the API actually returns using TweetService config"""
    config = get_api_config()
    
    # Use same query logic as TweetService
    clean_token = token_symbol.replace("$", "").strip()
    query_prefix = "#" if len(clean_token) > 6 else "$"
    query = f"{query_prefix}{clean_token}"
    
    params = {
        "type": "Latest",
        "count": "5",  # Small count for testing
        "query": query
    }
    
    async with httpx.AsyncClient(timeout=config["timeout"]) as client:
        try:
            logging.info(f"[debug] Making request to: {config['url']}")
            logging.info(f"[debug] Headers: {dict(config['headers'])}")
            logging.info(f"[debug] Params: {params}")
            
            response = await client.get(
                config["url"],
                headers=config["headers"],
                params=params
            )
            
            result = {
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "query_used": query,
                "endpoint": config["url"]
            }
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    result["json_parsed"] = True
                    result["data_type"] = str(type(data))
                    
                    if isinstance(data, dict):
                        result["data_keys"] = list(data.keys())
                        
                        # Check if it has the expected structure
                        if "result" in data:
                            timeline = data.get("result", {}).get("timeline", {})
                            if "instructions" in timeline:
                                instructions = timeline["instructions"]
                                result["instructions_count"] = len(instructions)
                                result["has_timeline_structure"] = True
                                
                                # Count entries
                                entry_count = 0
                                for instr in instructions:
                                    if instr.get("type") == "TimelineAddEntries":
                                        entry_count += len(instr.get("entries", []))
                                result["total_entries"] = entry_count
                                
                                # Try to parse tweets
                                tweets, cursor = _parse_api_response(data, token_symbol)
                                result["parsed_tweets_count"] = len(tweets)
                                result["next_cursor"] = cursor is not None
                                
                                if tweets:
                                    result["sample_tweet"] = {
                                        "tweet_id": tweets[0]["tweet_id"],
                                        "text_preview": tweets[0]["text"][:100],
                                        "user": tweets[0]["user"]["screen_name"],
                                        "created_at": tweets[0]["created_at"]
                                    }
                            else:
                                result["has_timeline_structure"] = False
                        else:
                            result["has_result_key"] = False
                    
                    # Don't include full data to avoid overwhelming response
                    result["response_preview"] = str(data)[:500] if data else "Empty response"
                    
                except Exception as json_error:
                    result["json_error"] = str(json_error)
                    result["response_text_preview"] = response.text[:500]
            else:
                result["error_text"] = response.text[:500]
            
            return result
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
                "query_used": query,
                "endpoint": config["url"]
            }

# Keep all other functions unchanged...
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