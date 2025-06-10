import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import httpx
import time
import os
from functools import lru_cache

from services.tweet_service import DateTimeHandler, is_relevant_tweet

# Active volume fetches in progress
in_progress_volume: Dict[str, asyncio.Future] = {}

# Cache for volume results
volume_cache = {}
VOLUME_CACHE_DURATION = 300  # 5 minutes

@lru_cache()
def get_volume_api_config() -> Dict[str, any]:
    """Get API configuration for volume service - SAME as TweetService"""
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    rapidapi_host = os.getenv("RAPIDAPI_HOST")
    
    if not rapidapi_key or not rapidapi_host:
        raise ValueError("RAPIDAPI_KEY and RAPIDAPI_HOST environment variables are required")
    
    # Use SAME endpoint as TweetService - NOT /v2/search
    api_url = f"https://{rapidapi_host}/search"
    
    return {
        "url": api_url,
        "headers": {
            "x-rapidapi-key": rapidapi_key,  # Same headers as TweetService (lowercase)
            "x-rapidapi-host": rapidapi_host
        },
        "timeout": int(os.getenv("API_TIMEOUT", "15")),
        "max_retries": int(os.getenv("API_MAX_RETRIES", "3")),
        "max_pages": int(os.getenv("VOLUME_MAX_PAGES", "20")),  
        "tweets_per_page": int(os.getenv("VOLUME_TWEETS_PER_PAGE", "20"))  # Same as TweetService
    }

async def handle_volume_count(token_symbol: str) -> dict:
    """Handle volume counting for a token using same API as TweetService with extensive pagination"""
    try:
        # Check cache first
        cache_key = f"volume_{token_symbol}_{int(time.time() // VOLUME_CACHE_DURATION)}"
        if cache_key in volume_cache:
            logging.info(f"[volume_cache] Returning cached result for {token_symbol}")
            return volume_cache[cache_key]
        
        # Fetch all tweets from the last 48 hours
        all_tweets = await _fetch_all_48h_tweets(token_symbol)
        
        if not all_tweets:
            logging.info(f"[volume] No tweets found for {token_symbol}")
            result = {
                "token": token_symbol,
                "total": 0,
                "buckets": {},
                "hours_covered": 0,
                "oldest_tweet_hours": 0,
                "message": "No tweets found"
            }
            volume_cache[cache_key] = result
            return result
        
        filtered_tweets = _filter_tweets_for_volume(all_tweets)
        
        if not filtered_tweets:
            logging.info(f"[volume] No tweets passed filtering for {token_symbol}")
            result = {
                "token": token_symbol,
                "total": 0,
                "buckets": {},
                "hours_covered": 0,
                "oldest_tweet_hours": 0,
                "message": "No tweets passed filtering"
            }
            volume_cache[cache_key] = result
            return result
        
        # Create hourly buckets
        hourly_buckets = _create_hourly_buckets(filtered_tweets)
        
        # Calculate coverage statistics
        now = DateTimeHandler.now()
        oldest_tweet = min(filtered_tweets, key=lambda t: t["parsed_created_at"])
        oldest_hours = (now - oldest_tweet["parsed_created_at"]).total_seconds() / 3600
        hours_covered = len(hourly_buckets)
        
        result = {
            "token": token_symbol,
            "total": len(filtered_tweets),
            "buckets": hourly_buckets,
            "hours_covered": hours_covered,
            "oldest_tweet_hours": round(oldest_hours, 1),
            "search_timestamp": now.isoformat()
        }
        
        # Cache the result
        volume_cache[cache_key] = result
        
        logging.info(f"[volume] Found {len(filtered_tweets)} filtered tweets for {token_symbol} "
                    f"covering {hours_covered} hours (oldest: {oldest_hours:.1f}h ago)")
        
        return result
        
    except Exception as e:
        logging.error(f"[volume] Error processing {token_symbol}: {e}")
        raise Exception(f"Volume count failed: {e}")

async def _fetch_all_48h_tweets(token_symbol: str) -> List[Dict]:
    """Fetch all tweets from the last 48 hours using SAME API as TweetService"""
    config = get_volume_api_config()
    all_tweets = []
    seen_ids = set()
    seen_cursors = set()
    cursor = None
    pages = 0
    
    # Target: 48 hours of tweets
    target_hours = 48
    cutoff_time = DateTimeHandler.now() - timedelta(hours=target_hours)
    
    logging.info(f"[volume] Starting extensive search for {token_symbol} (target: {target_hours}h)")
    
    async with httpx.AsyncClient(timeout=config["timeout"]) as client:
        while pages < config["max_pages"]:
            try:
                # Detect cursor cycles
                if cursor and cursor in seen_cursors:
                    logging.warning(f"[volume] Cursor cycle detected for {token_symbol}, breaking")
                    break
                
                if cursor:
                    seen_cursors.add(cursor)
                
                # Fetch page using SAME logic as TweetService
                page_tweets, next_cursor = await _fetch_tweets_page_same_api(
                    client, token_symbol, cursor, config
                )
                
                if not page_tweets:
                    logging.info(f"[volume] No more tweets available for {token_symbol}")
                    break
                
                # Process tweets and check if we're getting old enough data
                page_recent_tweets = []
                oldest_in_page = None
                
                for tweet in page_tweets:
                    if tweet.get("tweet_id") in seen_ids:
                        continue
                    
                    # Parse creation time using TweetService logic
                    created_at = DateTimeHandler.parse_twitter_time(tweet.get("created_at"))
                    if not created_at:
                        continue
                    
                    # Track oldest tweet in this page
                    if oldest_in_page is None or created_at < oldest_in_page:
                        oldest_in_page = created_at
                    
                    # Add parsed time for easier processing later
                    tweet["parsed_created_at"] = created_at
                    
                    seen_ids.add(tweet["tweet_id"])
                    page_recent_tweets.append(tweet)
                
                all_tweets.extend(page_recent_tweets)
                pages += 1
                
                # Check if we've reached our time target
                if oldest_in_page and oldest_in_page <= cutoff_time:
                    logging.info(f"[volume] Reached {target_hours}h target for {token_symbol} "
                               f"(oldest in page: {(DateTimeHandler.now() - oldest_in_page).total_seconds() / 3600:.1f}h ago)")
                    break
                
                # Log progress
                hours_back = (DateTimeHandler.now() - oldest_in_page).total_seconds() / 3600 if oldest_in_page else 0
                logging.info(f"[volume] [{token_symbol}] Page {pages}: {len(page_recent_tweets)} tweets "
                            f"(total: {len(all_tweets)}, oldest: {hours_back:.1f}h ago)")
                
                if not next_cursor:
                    logging.info(f"[volume] No more pages available for {token_symbol}")
                    break
                
                cursor = next_cursor
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logging.error(f"[volume] Error fetching page {pages + 1} for {token_symbol}: {e}")
                break
    
    # Final filtering to 48 hours
    filtered_by_time = [
        tweet for tweet in all_tweets 
        if tweet["parsed_created_at"] >= cutoff_time
    ]
    
    logging.info(f"[volume] Fetched {len(filtered_by_time)} tweets within {target_hours}h for {token_symbol} "
                f"across {pages} pages")
    
    return filtered_by_time

async def _fetch_tweets_page_same_api(client: httpx.AsyncClient, token_symbol: str, 
                                     cursor: Optional[str], config: Dict) -> tuple[List[Dict], Optional[str]]:
    """Fetch a single page using EXACT SAME API structure as TweetService"""
    try:
        # Use SAME query logic as TweetService
        clean_token = token_symbol.replace("$", "").strip()
        query_prefix = "#" if len(clean_token) > 6 else "$"
        query = f"{query_prefix}{clean_token}"
        
        # Use SAME parameters as TweetService
        params = {
            "type": "Latest",
            "count": str(config["tweets_per_page"]),
            "query": query
        }
        
        if cursor:
            params["cursor"] = cursor
        
        # Make request with SAME retry logic as TweetService
        response = await _make_request_with_retries(client, config, params, token_symbol)
        if not response:
            return [], None
        
        # Parse response using EXACT SAME logic as TweetService
        return _parse_api_response_same_as_tweet_service(response, token_symbol)
        
    except Exception as e:
        logging.error(f"[volume_api] Error fetching page for {token_symbol}: {e}")
        return [], None

async def _make_request_with_retries(client: httpx.AsyncClient, config: Dict, params: Dict, 
                                   token_symbol: str, attempt: int = 0) -> Optional[Dict]:
    """Make API request with retry logic - SAME as TweetService"""
    try:
        response = await client.get(
            config["url"],
            headers=config["headers"],
            params=params
        )
        
        if response.status_code == 429:
            if attempt < config["max_retries"]:
                delay = 2 ** attempt
                logging.warning(f"[volume] Rate limited for {token_symbol}, retrying in {delay}s...")
                await asyncio.sleep(delay)
                return await _make_request_with_retries(client, config, params, token_symbol, attempt + 1)
            else:
                logging.error(f"[volume] Rate limit exceeded for {token_symbol} after {config['max_retries']} retries")
                return None
        
        response.raise_for_status()
        return response.json()
        
    except httpx.HTTPStatusError as e:
        logging.error(f"[volume] HTTP error for {token_symbol}: {e.response.status_code}")
        if attempt < config["max_retries"]:
            delay = 2 ** attempt
            await asyncio.sleep(delay)
            return await _make_request_with_retries(client, config, params, token_symbol, attempt + 1)
        return None
        
    except httpx.RequestError as e:
        logging.warning(f"[volume] Request error for {token_symbol}: {e}")
        if attempt < config["max_retries"]:
            delay = 2 ** attempt
            await asyncio.sleep(delay)
            return await _make_request_with_retries(client, config, params, token_symbol, attempt + 1)
        return None
        
    except Exception as e:
        logging.error(f"[volume] Unexpected error for {token_symbol}: {e}")
        return None

def _parse_api_response_same_as_tweet_service(data: Dict[str, any], token_symbol: str) -> tuple[List[Dict], Optional[str]]:
    """Parse API response using EXACT SAME logic as TweetService"""
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
            
            # Extract cursor - SAME as TweetService
            if (content.get("entryType") == "TimelineTimelineCursor" and 
                content.get("cursorType") == "Bottom"):
                next_cursor = content.get("value")
                continue
            
            # Extract tweet data - SAME as TweetService
            item = content.get("itemContent", {})
            tweet_result = item.get("tweet_results", {}).get("result", {})
            legacy = tweet_result.get("legacy", {})
            user = (tweet_result.get("core", {})
                   .get("user_results", {})
                   .get("result", {})
                   .get("legacy", {}))
            
            if not legacy or not user:
                continue
            
            # Validate required fields - SAME as TweetService
            tweet_id = legacy.get("id_str")
            text = legacy.get("full_text")
            created_at = legacy.get("created_at")
            screen_name = user.get("screen_name")
            
            if not all([tweet_id, text, created_at, screen_name]):
                continue
            
            # Create tweet object in expected format - SAME as TweetService
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
        
        logging.info(f"[volume] Parsed {len(tweets)} tweets for {token_symbol}")
        return tweets, next_cursor
        
    except Exception as e:
        logging.error(f"[volume] Failed to parse API response for {token_symbol}: {e}")
        return [], None

def _filter_tweets_for_volume(tweets: List[Dict]) -> List[Dict]:
    """Filter tweets using the same criteria as tweet service but without sentiment analysis"""
    filtered = []
    
    for tweet in tweets:
        try:
            # Check minimum followers - SAME as TweetService (150)
            user = tweet.get("user", {})
            followers_count = user.get("followers_count", 0)
            if followers_count < 150:  
                continue
            
            # Check relevance using SAME function as TweetService
            text = tweet.get("text", "")
            if not is_relevant_tweet(text):
                continue
            
            # Basic validation
            if not tweet.get("tweet_id") or not text.strip():
                continue
            
            filtered.append(tweet)
            
        except Exception as e:
            logging.warning(f"[volume_filter] Error filtering tweet {tweet.get('tweet_id')}: {e}")
            continue
    
    return filtered

def _create_hourly_buckets(tweets: List[Dict]) -> Dict[str, int]:
    """Create hourly buckets from filtered tweets"""
    hourly_buckets = {}
    
    for tweet in tweets:
        try:
            created_at = tweet["parsed_created_at"]
            # Round down to the hour
            hour_key = created_at.replace(minute=0, second=0, microsecond=0).isoformat()
            hourly_buckets[hour_key] = hourly_buckets.get(hour_key, 0) + 1
        except Exception as e:
            logging.warning(f"[volume_buckets] Error bucketing tweet {tweet.get('tweet_id')}: {e}")
            continue
    
    return hourly_buckets

def get_or_create_volume_future(token_symbol: str, queue: asyncio.Queue) -> asyncio.Future:
    """Get existing future or create new one for volume search"""
    token_symbol = token_symbol.lower()
    loop = asyncio.get_event_loop()
    
    if token_symbol in in_progress_volume:
        logging.info(f"[volume] Reusing in-progress search for {token_symbol}")
        return in_progress_volume[token_symbol]
    
    future = loop.create_future()
    in_progress_volume[token_symbol] = future
    
    try:
        queue.put_nowait((token_symbol, future))
        logging.info(f"[volume_queue] Queued volume search for {token_symbol}")
    except asyncio.QueueFull:
        logging.warning(f"[volume_queue] Queue full, rejecting search for {token_symbol}")
        future.set_exception(Exception("Volume queue is full"))
        in_progress_volume.pop(token_symbol, None)
    
    return future

async def process_volume_queue(queue: asyncio.Queue):
    """Process volume requests from the queue"""
    if queue.empty():
        return
    
    try:
        token_symbol, future = await asyncio.wait_for(queue.get(), timeout=1.0)
        logging.info(f"[volume_queue] Processing tweet volume for {token_symbol}")
        
        try:
            result = await handle_volume_count(token_symbol)
            if not future.done():
                future.set_result(result)
                logging.info(f"[volume_queue] Successfully completed volume search for {token_symbol}")
        except Exception as e:
            logging.error(f"[volume_queue] Failed to process {token_symbol}: {e}")
            if not future.done():
                future.set_exception(e)
        finally:
            # Always clean up the in_progress entry
            in_progress_volume.pop(token_symbol, None)
            
    except asyncio.TimeoutError:
        # No items in queue, this is normal
        pass
    except Exception as e:
        logging.error(f"[volume_queue] Error processing queue: {e}")

async def get_volume_queue_status(queue: asyncio.Queue) -> Dict[str, any]:
    """Get status of the volume queue"""
    return {
        "queue_size": queue.qsize(),
        "in_progress_volume_searches": len(in_progress_volume),
        "in_progress_tokens": list(in_progress_volume.keys()),
        "queue_maxsize": queue.maxsize,
        "is_queue_full": queue.full()
    }

async def clear_volume_cache():
    """Clear the volume cache and in-progress searches"""
    global in_progress_volume, volume_cache
    cleared_count = len(in_progress_volume)
    cleared_cache = len(volume_cache)
    
    # Cancel any pending futures
    for future in in_progress_volume.values():
        if not future.done():
            future.cancel()
    
    in_progress_volume.clear()
    volume_cache.clear()
    
    logging.info(f"[volume_cache] Cleared {cleared_count} in-progress searches and {cleared_cache} cached results")
    return {"cleared_searches": cleared_count, "cleared_cache_entries": cleared_cache}

# Debug function using SAME API as TweetService
async def debug_volume_api_response(token_symbol: str = "sse") -> dict:
    """Debug function to inspect what the volume API returns using SAME endpoint as TweetService"""
    config = get_volume_api_config()
    
    # Use SAME query logic as TweetService
    clean_token = token_symbol.replace("$", "").strip()
    query_prefix = "#" if len(clean_token) > 6 else "$"
    query = f"{query_prefix}{clean_token}"
    
    # Use SAME parameters as TweetService
    params = {
        "type": "Latest",
        "count": "5",  # Small count for testing
        "query": query
    }
    
    async with httpx.AsyncClient(timeout=config["timeout"]) as client:
        try:
            logging.info(f"[volume_debug] Making request to: {config['url']}")
            logging.info(f"[volume_debug] Headers: {dict(config['headers'])}")
            logging.info(f"[volume_debug] Params: {params}")
            
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
                                
                                # Try to parse tweets using SAME logic as TweetService
                                tweets, cursor = _parse_api_response_same_as_tweet_service(data, token_symbol)
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