import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import aiohttp
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
    """Get API configuration for volume service"""
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    rapidapi_host = os.getenv("RAPIDAPI_HOST", "twitter241.p.rapidapi.com")
    api_url = f"https://{rapidapi_host}/v2/search"
    
    if not rapidapi_key:
        raise ValueError("RAPIDAPI_KEY environment variable is required")
    
    return {
        "url": api_url,
        "headers": {
            "X-RapidAPI-Key": rapidapi_key,
            "X-RapidAPI-Host": rapidapi_host
        },
        "timeout": int(os.getenv("API_TIMEOUT", "15")),
        "max_retries": int(os.getenv("API_MAX_RETRIES", "2")),
        "max_pages": int(os.getenv("VOLUME_MAX_PAGES", "20")),  
        "tweets_per_page": int(os.getenv("VOLUME_TWEETS_PER_PAGE", "100"))
    }

async def handle_volume_count(token_symbol: str) -> dict:
    """Handle volume counting for a token using new API with extensive pagination"""
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
        
        # Filter tweets using the same criteria as tweet service
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
    """Fetch all tweets from the last 48 hours with extensive pagination"""
    config = get_volume_api_config()
    all_tweets = []
    seen_ids = set()
    cursor = None
    pages = 0
    
    # Target: 48 hours of tweets
    target_hours = 48
    cutoff_time = DateTimeHandler.now() - timedelta(hours=target_hours)
    
    logging.info(f"[volume] Starting extensive search for {token_symbol} (target: {target_hours}h)")
    
    while pages < config["max_pages"]:
        try:
            # Fetch page
            page_tweets, next_cursor = await _fetch_tweets_page(token_symbol, cursor)
            
            if not page_tweets:
                logging.info(f"[volume] No more tweets available for {token_symbol}")
                break
            
            # Process tweets and check if we're getting old enough data
            page_recent_tweets = []
            oldest_in_page = None
            
            for tweet in page_tweets:
                if tweet.get("tweet_id") in seen_ids:
                    continue
                
                # Parse creation time
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

async def _fetch_tweets_page(token_symbol: str, cursor: Optional[str] = None, retries: int = 0) -> tuple[List[Dict], Optional[str]]:
    """Fetch a single page of tweets using the actual API structure"""
    config = get_volume_api_config()
    
    async with aiohttp.ClientSession() as session:
        try:
            # Remove $ prefix to get clean token for API query construction
            clean_token = token_symbol.replace("$", "").strip()
            
            # Use $ prefix for the search query
            query = f"${clean_token}"
            
            params = {
                "query": query,
                "count": config["tweets_per_page"],
                "result_type": "recent"
            }
            
            # Add cursor for pagination if available
            if cursor:
                params["cursor"] = cursor
            
            async with session.get(
                config["url"],
                headers=config["headers"],
                params=params,
                timeout=aiohttp.ClientTimeout(total=config["timeout"])
            ) as response:
                
                if response.status == 429 and retries < config["max_retries"]:
                    # Rate limited, wait and retry
                    wait_time = 2 ** retries
                    logging.warning(f"[volume_api] Rate limited, waiting {wait_time}s before retry")
                    await asyncio.sleep(wait_time)
                    return await _fetch_tweets_page(token_symbol, cursor, retries + 1)
                
                # Check content type and handle empty responses
                content_type = response.headers.get('content-type', '').lower()
                if 'application/json' not in content_type:
                    logging.error(f"[volume_api] Unexpected content type for {token_symbol}: {content_type}")
                    return [], None
                
                response_text = await response.text()
                if not response_text or response_text.strip() == '':
                    logging.warning(f"[volume_api] Empty response body for {token_symbol}")
                    return [], None
                
                try:
                    data = await response.json()
                except Exception as json_error:
                    logging.error(f"[volume_api] JSON parsing failed for {token_symbol}: {json_error}")
                    return [], None
                
                if data is None or not isinstance(data, dict):
                    logging.warning(f"[volume_api] Invalid response structure for {token_symbol}")
                    return [], None
                
                response.raise_for_status()
                
                # Parse the actual API response structure
                tweets = []
                next_cursor = None
                
                # Navigate through the actual response structure
                result = data.get("result", {})
                timeline = result.get("timeline", {})
                instructions = timeline.get("instructions", [])
                
                for instruction in instructions:
                    if instruction.get("type") == "TimelineAddEntries":
                        entries = instruction.get("entries", [])
                        
                        for entry in entries:
                            entry_id = entry.get("entryId", "")
                            
                            # Handle cursor entries for pagination
                            if entry_id.startswith("cursor-bottom-"):
                                content = entry.get("content", {})
                                if content.get("entryType") == "TimelineTimelineCursor":
                                    next_cursor = content.get("value")
                                continue
                                
                            # Skip top cursor and other non-tweet entries
                            if entry_id.startswith("cursor-") or not entry_id.startswith("tweet-"):
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
                            
                            # Parse created_at
                            created_at_str = legacy.get("created_at")
                            if not created_at_str:
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
                
                return tweets, next_cursor
                
        except aiohttp.ClientError as e:
            if retries < config["max_retries"]:
                wait_time = 2 ** retries
                logging.warning(f"[volume_api] Request failed, retrying in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)
                return await _fetch_tweets_page(token_symbol, cursor, retries + 1)
            else:
                logging.error(f"[volume_api] API request failed after {retries} retries: {e}")
                return [], None
        except Exception as e:
            logging.error(f"[volume_api] Unexpected error for {token_symbol}: {e}")
            return [], None

def _filter_tweets_for_volume(tweets: List[Dict]) -> List[Dict]:
    """Filter tweets using the same criteria as tweet service but without sentiment analysis"""
    filtered = []
    
    for tweet in tweets:
        try:
            # Check minimum followers
            user = tweet.get("user", {})
            followers_count = user.get("followers_count", 0)
            if followers_count < 150:  
                continue
            
            # Check relevance 
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

# Add debugging function for volume service
async def debug_volume_api_response(token_symbol: str = "ibrl") -> dict:
    """Debug function to inspect what the volume API actually returns"""
    config = get_volume_api_config()
    
    async with aiohttp.ClientSession() as session:
        try:
            clean_token = token_symbol.replace("$", "").strip()
            query = f"${clean_token}"
            
            params = {
                "query": query,
                "count": 5,  # Small count for testing
                "result_type": "recent"
            }
            
            logging.info(f"[volume_debug] Making request to: {config['url']}")
            logging.info(f"[volume_debug] Headers: {config['headers']}")
            logging.info(f"[volume_debug] Params: {params}")
            
            async with session.get(
                config["url"],
                headers=config["headers"],
                params=params,
                timeout=aiohttp.ClientTimeout(total=config["timeout"])
            ) as response:
                
                logging.info(f"[volume_debug] Response status: {response.status}")
                logging.info(f"[volume_debug] Response headers: {dict(response.headers)}")
                
                # Get raw response text first
                response_text = await response.text()
                logging.info(f"[volume_debug] Raw response length: {len(response_text)}")
                logging.info(f"[volume_debug] Raw response preview: {response_text[:1000]}")
                
                # Try to parse as JSON
                try:
                    if response_text:
                        import json
                        data = json.loads(response_text)
                        
                        # Check for pagination cursors
                        result = data.get("result", {})
                        timeline = result.get("timeline", {})
                        instructions = timeline.get("instructions", [])
                        
                        cursor_info = {}
                        tweet_count = 0
                        
                        for instruction in instructions:
                            if instruction.get("type") == "TimelineAddEntries":
                                entries = instruction.get("entries", [])
                                for entry in entries:
                                    entry_id = entry.get("entryId", "")
                                    if entry_id.startswith("cursor-"):
                                        content = entry.get("content", {})
                                        cursor_info[entry_id] = content.get("value")
                                    elif entry_id.startswith("tweet-"):
                                        tweet_count += 1
                        
                        return {
                            "status": "success",
                            "response_length": len(response_text),
                            "data_type": str(type(data)),
                            "data_keys": list(data.keys()) if isinstance(data, dict) else None,
                            "tweet_count": tweet_count,
                            "cursor_info": cursor_info,
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
            logging.error(f"[volume_debug] Error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }