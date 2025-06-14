import asyncio
import os
import httpx
import logging
import json
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Tuple, Any
from enum import Enum
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline
from sqlalchemy import delete, select, and_
import math
import re

from db import database
from models import tokens, tweets
from utils import is_relevant_tweet

class Config:
    RATE_LIMIT = 10
    REQUEST_TIMEOUT = 10.0
    MAX_RETRIES = 3
    INITIAL_RETRY_DELAY = 1
    
    TWEET_TIME_WINDOW_HOURS = 48
    MAX_FETCH_PAGES = 10
    MAX_TWEETS_PER_FETCH = 200
    TWEETS_PER_PAGE = 20
    
    MAX_OPTIMIZED_PAGES = 3
    OPTIMIZED_TIME_BUFFER_MINUTES = 60
    
    MIN_FOLLOWERS = 150
    MAX_MEMORY_CACHE_SIZE = 10000
    
    MODEL_NAME = "ElKulako/cryptobert"
    SENTIMENT_MULTIPLIER = 88
    MAX_RAW_SCORE = 2.5
    
    DECAY_HOURS = 12
    PRIOR_SCORE = 50.0
    BAYESIAN_CONFIDENCE = 15
    VOLUME_BOOST_MIDPOINT = 5
    MAX_WOM_SCORE = 100.0

@dataclass
class UserInfo:
    screen_name: str
    followers_count: int
    avatar: str = ""

@dataclass
class RawTweet:
    tweet_id: str
    text: str
    created_at: datetime
    user_info: UserInfo

@dataclass
class ProcessedTweet:
    tweet_id: str
    token_symbol: str
    text: str
    user_name: str
    followers_count: int
    profile_pic: str
    created_at: datetime
    wom_score: float
    tweet_url: str

@dataclass
class SentimentResult:
    wom_score: float
    tweet_count: int
    tweets: List[ProcessedTweet]

class ServiceError(Exception):
    pass

class APIError(ServiceError):
    pass

class ValidationError(ServiceError):
    pass

class DatabaseError(ServiceError):
    pass

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()

class DateTimeHandler:
    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc)
    
    @staticmethod
    def parse_twitter_time(raw_time: str) -> Optional[datetime]:
        if not raw_time or not isinstance(raw_time, str):
            return None
        
        try:
            dt = datetime.fromisoformat(raw_time.replace('Z', '+00:00'))
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
        
        try:
            dt = datetime.strptime(raw_time, "%a %b %d %H:%M:%S %z %Y")
            return dt.astimezone(timezone.utc)
        except ValueError:
            logger.warning(f"Failed to parse timestamp: {raw_time}")
            return None
    
    @staticmethod
    def is_within_hours(dt: datetime, hours: int) -> bool:
        if not dt:
            return False
        cutoff = DateTimeHandler.now() - timedelta(hours=hours)
        return dt >= cutoff

class TextCleaner:
    @staticmethod
    def clean_text(text: str) -> str:
        if not text:
            return ""
        
        text = re.sub(r"http\S+", "", text)
        text = re.sub(r"[^\w\s\$#@]", "", text)
        return text.strip()

class ValidationHelper:
    @staticmethod
    def validate_tweet_data(data: Dict[str, Any]) -> bool:
        required_fields = ['tweet_id', 'text', 'created_at']
        return all(
            field in data and data[field] is not None 
            for field in required_fields
        )
    
    @staticmethod
    def validate_user_data(data: Dict[str, Any]) -> bool:
        required_fields = ['screen_name']
        return all(
            field in data and data[field] is not None 
            for field in required_fields
        )

class SentimentAnalyzer:
    def __init__(self):
        self.tokenizer = None
        self.model = None
        self.pipe = None
        self._initialized = False
    
    async def initialize(self):
        if self._initialized:
            return
        
        try:
            logger.info("Initializing CryptoBERT sentiment model...")
            self.tokenizer = AutoTokenizer.from_pretrained(Config.MODEL_NAME)
            self.model = AutoModelForSequenceClassification.from_pretrained(Config.MODEL_NAME)
            self.pipe = TextClassificationPipeline(
                model=self.model, 
                tokenizer=self.tokenizer, 
                top_k=None
            )
            self._initialized = True
            logger.info("Sentiment model initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize sentiment model: {e}")
            raise ServiceError(f"Sentiment model initialization failed: {e}")
    
    async def analyze_sentiment(self, text: str) -> float:
        if not self._initialized:
            await self.initialize()
        
        if not text:
            return 1.0
        
        try:
            cleaned_text = TextCleaner.clean_text(text)
            if not cleaned_text:
                return 1.0
            
            predictions = self.pipe(cleaned_text)[0]
            scores = {pred["label"].lower(): pred["score"] for pred in predictions}
            
            positive = scores.get("bullish", 0.0)
            neutral = scores.get("neutral", 0.0)
            
            raw_score = (2.0 * positive) + (0.5 * neutral)
            normalized = min(raw_score, Config.MAX_RAW_SCORE) / Config.MAX_RAW_SCORE * Config.SENTIMENT_MULTIPLIER
            
            return round(normalized, 2)
            
        except Exception as e:
            logger.error(f"Sentiment analysis error: {e}")
            return 1.0

class TwitterAPIClient:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(Config.RATE_LIMIT)
        self.rapidapi_key = os.getenv("RAPIDAPI_KEY")
        self.rapidapi_host = os.getenv("RAPIDAPI_HOST")
        
        if not self.rapidapi_key or not self.rapidapi_host:
            raise ServiceError("Missing RAPIDAPI_KEY or RAPIDAPI_HOST environment variables")
    
    @asynccontextmanager
    async def _rate_limited_request(self):
        async with self.semaphore:
            yield
    
    async def fetch_tweets(self, token_symbol: str, cursor: Optional[str] = None) -> Tuple[List[RawTweet], Optional[str]]:
        if not token_symbol:
            raise ValidationError("Token symbol cannot be empty")
        
        clean_token = token_symbol.replace("$", "").strip()
        query_prefix = "#" if len(clean_token) > 6 else "$"
        query = f"{query_prefix}{clean_token}"
        
        return await self._fetch_tweets_page(query, cursor)
    
    async def fetch_recent_tweets_optimized(self, token_symbol: str, last_seen_tweet_id: Optional[str] = None) -> List[RawTweet]:
        clean_token = token_symbol.replace("$", "").strip()
        query_prefix = "#" if len(clean_token) > 6 else "$"
        query = f"{query_prefix}{clean_token}"
        
        logger.info(f"Fetching recent tweets for '{query}' (last seen: {last_seen_tweet_id})")
        
        all_tweets = []
        seen_ids = set()
        cursor = None
        pages = 0
        
        cutoff_time = DateTimeHandler.now() - timedelta(minutes=Config.OPTIMIZED_TIME_BUFFER_MINUTES)
        last_seen_id_int = int(last_seen_tweet_id) if last_seen_tweet_id else 0
        
        found_newer_tweets = False
        consecutive_old_tweets = 0
        
        while pages < Config.MAX_OPTIMIZED_PAGES:
            try:
                page_tweets, next_cursor = await self._fetch_tweets_page(query, cursor)
                
                if not page_tweets:
                    logger.debug(f"No more tweets available for '{query}' after {pages} pages")
                    break
                
                new_tweets_in_page = []
                old_tweets_in_page = 0
                
                for tweet in page_tweets:
                    if tweet.tweet_id in seen_ids:
                        continue
                    
                    try:
                        current_id_int = int(tweet.tweet_id)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid tweet ID format: {tweet.tweet_id}")
                        continue
                    
                    if current_id_int > last_seen_id_int:
                        found_newer_tweets = True
                        consecutive_old_tweets = 0
                        
                        if tweet.created_at < cutoff_time:
                            logger.debug(f"Tweet {tweet.tweet_id} is newer than last seen but older than {Config.OPTIMIZED_TIME_BUFFER_MINUTES} minutes")
                            continue
                        
                        new_tweets_in_page.append(tweet)
                        seen_ids.add(tweet.tweet_id)
                    else:
                        old_tweets_in_page += 1
                        consecutive_old_tweets += 1
                
                all_tweets.extend(new_tweets_in_page)
                pages += 1
                
                logger.debug(f"Page {pages}: {len(new_tweets_in_page)} new tweets, {old_tweets_in_page} old tweets")
                
                if last_seen_tweet_id and consecutive_old_tweets >= 10:
                    logger.debug(f"Found {consecutive_old_tweets} consecutive old tweets, stopping fetch")
                    break
                
                if last_seen_tweet_id and len(new_tweets_in_page) == 0 and old_tweets_in_page > 0:
                    logger.debug(f"No new tweets in page {pages}, stopping fetch")
                    break
                
                if not next_cursor:
                    logger.debug(f"No more pages available")
                    break
                
                cursor = next_cursor
                await asyncio.sleep(0.05)
                
            except Exception as e:
                logger.error(f"Error on page {pages + 1} for '{query}': {e}")
                break
        
        all_tweets.sort(key=lambda x: int(x.tweet_id), reverse=True)
        
        logger.info(f"Fetched {len(all_tweets)} recent tweets for '{query}' from {pages} pages (found_newer: {found_newer_tweets})")
        return all_tweets
    
    async def _fetch_tweets_page(self, query: str, cursor: Optional[str] = None) -> Tuple[List[RawTweet], Optional[str]]:
        url = f"https://{self.rapidapi_host}/search"
        headers = {
            "x-rapidapi-key": self.rapidapi_key,
            "x-rapidapi-host": self.rapidapi_host
        }
        params = {
            "type": "Latest",
            "count": str(Config.TWEETS_PER_PAGE),
            "query": query
        }
        
        if cursor:
            params["cursor"] = cursor
        
        for attempt in range(Config.MAX_RETRIES):
            async with self._rate_limited_request():
                try:
                    async with httpx.AsyncClient(timeout=Config.REQUEST_TIMEOUT) as client:
                        response = await client.get(url, headers=headers, params=params)
                        
                        if response.status_code == 429:
                            delay = Config.INITIAL_RETRY_DELAY * (2 ** attempt)
                            logger.warning(f"Rate limited for query '{query}', retrying in {delay}s...")
                            await asyncio.sleep(delay)
                            continue
                        
                        response.raise_for_status()
                        return self._parse_api_response(response.json(), query)
                        
                except httpx.HTTPStatusError as e:
                    logger.error(f"HTTP error for query '{query}': {e.response.status_code}")
                    if attempt == Config.MAX_RETRIES - 1:
                        raise APIError(f"HTTP error: {e.response.status_code}")
                        
                except httpx.RequestError as e:
                    logger.warning(f"Request error for query '{query}': {e}")
                    if attempt == Config.MAX_RETRIES - 1:
                        raise APIError(f"Request error: {e}")
                        
                except Exception as e:
                    logger.error(f"Unexpected error for query '{query}': {e}")
                    if attempt == Config.MAX_RETRIES - 1:
                        raise APIError(f"Unexpected error: {e}")
                
                delay = Config.INITIAL_RETRY_DELAY * (2 ** attempt)
                await asyncio.sleep(delay)
        
        raise APIError(f"Failed after {Config.MAX_RETRIES} retries")
    
    def _parse_api_response(self, data: Dict[str, Any], token_symbol: str) -> Tuple[List[RawTweet], Optional[str]]:
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
                
                if (content.get("entryType") == "TimelineTimelineCursor" and 
                    content.get("cursorType") == "Bottom"):
                    next_cursor = content.get("value")
                    continue
                
                item = content.get("itemContent", {})
                tweet_result = item.get("tweet_results", {}).get("result", {})
                
                if tweet_result.get("__typename") == "TweetWithVisibilityResults":
                    tweet_result = tweet_result.get("tweet", {})
                
                legacy = tweet_result.get("legacy", {})
                user = (tweet_result.get("core", {})
                       .get("user_results", {})
                       .get("result", {})
                       .get("legacy", {}))
                
                if not legacy or not user:
                    continue
                
                tweet_data = {
                    "tweet_id": legacy.get("id_str"),
                    "text": legacy.get("full_text"),
                    "created_at": legacy.get("created_at"),
                    "user_info": {
                        "screen_name": user.get("screen_name"),
                        "followers_count": user.get("followers_count", 0),
                        "avatar": user.get("profile_image_url_https", "")
                    }
                }
                
                if not ValidationHelper.validate_tweet_data(tweet_data):
                    logger.warning(f"Invalid tweet data for {token_symbol}: {tweet_data.get('tweet_id')}")
                    continue
                
                if not ValidationHelper.validate_user_data(tweet_data["user_info"]):
                    logger.warning(f"Invalid user data for {token_symbol}: {tweet_data.get('tweet_id')}")
                    continue
                
                created_at = DateTimeHandler.parse_twitter_time(tweet_data["created_at"])
                if not created_at:
                    logger.warning(f"Failed to parse datetime for tweet {tweet_data['tweet_id']}")
                    continue
                
                user_info = UserInfo(**tweet_data["user_info"])
                raw_tweet = RawTweet(
                    tweet_id=tweet_data["tweet_id"],
                    text=tweet_data["text"],
                    created_at=created_at,
                    user_info=user_info
                )
                tweets.append(raw_tweet)
            
            logger.debug(f"Parsed {len(tweets)} tweets for query '{token_symbol}'")
            return tweets, next_cursor
            
        except Exception as e:
            logger.error(f"Failed to parse API response for query '{token_symbol}': {e}")
            raise APIError(f"Response parsing failed: {e}")

class TweetProcessor:
    def __init__(self, sentiment_analyzer: SentimentAnalyzer):
        self.sentiment_analyzer = sentiment_analyzer
    
    async def process_tweets(self, raw_tweets: List[RawTweet], token_symbol: str) -> List[ProcessedTweet]:
        if not raw_tweets:
            return []
        
        filtered_tweets = self._filter_tweets(raw_tweets)
        if not filtered_tweets:
            logger.info(f"No tweets passed filtering for {token_symbol}")
            return []
        
        texts = [tweet.text for tweet in filtered_tweets]
        sentiment_scores = await asyncio.gather(
            *(self.sentiment_analyzer.analyze_sentiment(text) for text in texts),
            return_exceptions=True
        )
        
        processed_tweets = []
        
        for tweet, score in zip(filtered_tweets, sentiment_scores):
            if isinstance(score, Exception):
                logger.error(f"Sentiment analysis failed for tweet {tweet.tweet_id}: {score}")
                score = 1.0
            
            processed_tweet = ProcessedTweet(
                tweet_id=tweet.tweet_id,
                token_symbol=token_symbol,
                text=tweet.text.strip(),
                user_name=tweet.user_info.screen_name,
                followers_count=tweet.user_info.followers_count,
                profile_pic=tweet.user_info.avatar,
                created_at=tweet.created_at,
                wom_score=float(score),
                tweet_url=f"https://x.com/{tweet.user_info.screen_name}/status/{tweet.tweet_id}"
            )
            processed_tweets.append(processed_tweet)
        
        logger.info(f"Processed {len(processed_tweets)} tweets with sentiment for {token_symbol}")
        return processed_tweets
    
    def _filter_tweets(self, raw_tweets: List[RawTweet]) -> List[RawTweet]:
        filtered = []
        
        for tweet in raw_tweets:
            if tweet.user_info.followers_count < Config.MIN_FOLLOWERS:
                continue
            
            if not is_relevant_tweet(tweet.text):
                continue
            
            if not DateTimeHandler.is_within_hours(tweet.created_at, Config.TWEET_TIME_WINDOW_HOURS):
                continue
            
            filtered.append(tweet)
        
        return filtered

class DatabaseManager:
    async def get_active_tokens(self) -> List[str]:
        try:
            query = tokens.select().where(tokens.c.is_active == True)
            rows = await database.fetch_all(query)
            return [row["token_symbol"] for row in rows]
        except Exception as e:
            logger.error(f"Failed to fetch active tokens: {e}")
            raise DatabaseError(f"Failed to fetch active tokens: {e}")
    
    async def get_last_seen_tweet_id(self, token_symbol: str) -> Optional[str]:
        try:
            if not token_symbol.startswith('$'):
                token_symbol = f'${token_symbol}'
                
            query = """
                SELECT tweet_id::text as tweet_id
                FROM tweets 
                WHERE token_symbol = :token_symbol 
                ORDER BY tweet_id DESC 
                LIMIT 1
            """
            
            result = await database.fetch_one(query, {"token_symbol": token_symbol})
            return result["tweet_id"] if result else None
            
        except Exception as e:
            logger.error(f"Failed to get last seen tweet ID for {token_symbol}: {e}")
            return None
    
    async def get_existing_tweet_ids(self, token_symbol: str) -> set:
        try:    
            query = """
                SELECT tweet_id::text as tweet_id
                FROM tweets 
                WHERE token_symbol = :token_symbol
            """
            rows = await database.fetch_all(query, {"token_symbol": token_symbol})
            return {row["tweet_id"] for row in rows}
        except Exception as e:
            logger.error(f"Failed to fetch existing tweet IDs for {token_symbol}: {e}")
            raise DatabaseError(f"Failed to fetch existing tweet IDs: {e}")
    
    async def store_tweets(self, processed_tweets: List[ProcessedTweet]) -> int:
        if not processed_tweets:
            return 0
        
        try:
            tweet_records = [self._tweet_to_record(tweet) for tweet in processed_tweets]
            
            query = """
                INSERT INTO tweets (
                    tweet_id, token_symbol, text, user_name, followers_count, 
                    profile_pic, created_at, wom_score, tweet_url
                )
                VALUES (
                    :tweet_id::BIGINT, :token_symbol, :text, :user_name, :followers_count,
                    :profile_pic, :created_at, :wom_score, :tweet_url
                )
                ON CONFLICT (tweet_id) DO NOTHING
            """
            
            result = await database.execute_many(query=query, values=tweet_records)
            logger.info(f"Stored {len(tweet_records)} tweets in database")
            return len(tweet_records)
            
        except Exception as e:
            logger.error(f"Failed to store tweets: {e}")
            raise DatabaseError(f"Failed to store tweets: {e}")
    
    def _tweet_to_record(self, tweet: ProcessedTweet) -> Dict[str, Any]:
        return {
            "tweet_id": tweet.tweet_id,
            "token_symbol": tweet.token_symbol,
            "text": tweet.text,
            "user_name": tweet.user_name,
            "followers_count": tweet.followers_count,
            "profile_pic": tweet.profile_pic,
            "created_at": tweet.created_at,
            "wom_score": tweet.wom_score,
            "tweet_url": tweet.tweet_url
        }
    
    async def get_recent_tweets(self, token_symbol: str, hours: int = Config.TWEET_TIME_WINDOW_HOURS) -> List[Dict[str, Any]]:
        try:
            if not token_symbol.startswith('$'):
                token_symbol = f'${token_symbol}'
                
            cutoff = DateTimeHandler.now() - timedelta(hours=hours)
            
            query = """
                SELECT 
                    tweet_id::text as tweet_id,
                    token_symbol,
                    text,
                    user_name,
                    followers_count,
                    profile_pic,
                    created_at,
                    wom_score,
                    tweet_url
                FROM tweets
                WHERE token_symbol = :token_symbol
                AND created_at >= :cutoff
                AND wom_score IS NOT NULL
                ORDER BY created_at DESC
            """
            
            rows = await database.fetch_all(query, {
                "token_symbol": token_symbol,
                "cutoff": cutoff
            })
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to fetch recent tweets for {token_symbol}: {e}")
            raise DatabaseError(f"Failed to fetch recent tweets: {e}")
    
    async def update_token_metrics(self, token_symbol: str, wom_score: float, tweet_count: int, 
                             avg_followers: Optional[int] = None, first_spotted_by: Optional[str] = None):
        try:
            if not token_symbol.startswith('$'):
                token_symbol = f'${token_symbol}'
            
            values = {
                "wom_score": wom_score,
                "tweet_count": tweet_count,
            }
            
            if avg_followers is not None:
                values["avg_followers_count"] = avg_followers
            
            if first_spotted_by:
                values["first_spotted_by"] = first_spotted_by
            
            query = tokens.update().where(
                tokens.c.token_symbol == token_symbol
            ).values(values)
            
            await database.execute(query)
            logger.info(f"Updated metrics for {token_symbol}: WOM={wom_score}, count={tweet_count}")
            
        except Exception as e:
            logger.error(f"Failed to update token metrics for {token_symbol}: {e}")
            raise DatabaseError(f"Failed to update token metrics: {e}")
    
    async def prune_old_tweets(self, hours: int = Config.TWEET_TIME_WINDOW_HOURS):
        try:
            cutoff = DateTimeHandler.now() - timedelta(hours=hours)
            query = delete(tweets).where(tweets.c.created_at < cutoff)
            result = await database.execute(query)
            logger.info(f"Pruned tweets older than {hours} hours")
            return result
        except Exception as e:
            logger.error(f"Failed to prune old tweets: {e}")
            raise DatabaseError(f"Failed to prune old tweets: {e}")

class WOMCalculator:
    @staticmethod
    def calculate_volume_boost(count: int, midpoint: int = Config.VOLUME_BOOST_MIDPOINT) -> float:
        return 1 / (1 + math.exp(-0.5 * (count - midpoint)))
    
    @staticmethod
    def calculate_final_wom_score(tweet_records: List[Dict[str, Any]]) -> float:
        if not tweet_records:
            return 1.0
        
        now = DateTimeHandler.now()
        weight_sum = 0.0
        score_sum = 0.0
        
        for record in tweet_records:
            created_at = record["created_at"]
            if isinstance(created_at, str):
                created_at = DateTimeHandler.parse_twitter_time(created_at)
            
            if not created_at:
                continue
            
            age_hours = (now - created_at).total_seconds() / 3600
            weight = math.exp(-age_hours / Config.DECAY_HOURS)
            score = float(record.get("wom_score", 0))
            
            score_sum += score * weight
            weight_sum += weight
        
        avg_score = score_sum / weight_sum if weight_sum > 0 else Config.PRIOR_SCORE
        
        tweet_count = len(tweet_records)
        smoothed_score = (
            (Config.BAYESIAN_CONFIDENCE * Config.PRIOR_SCORE + tweet_count * avg_score) /
            (Config.BAYESIAN_CONFIDENCE + tweet_count)
        )
        
        volume_weight = WOMCalculator.calculate_volume_boost(tweet_count)
        final_score = smoothed_score * volume_weight
        
        return round(min(final_score, Config.MAX_WOM_SCORE), 2)
    
    @staticmethod
    def calculate_average_followers(tweet_records: List[Dict[str, Any]]) -> int:
        if not tweet_records:
            return 0
        
        valid_counts = [
            record.get("followers_count", 0) 
            for record in tweet_records 
            if record.get("followers_count") is not None
        ]
        
        return int(sum(valid_counts) / len(valid_counts)) if valid_counts else 0
    
    @staticmethod
    def find_first_spotted_by(tweet_records: List[Dict[str, Any]]) -> Optional[str]:
        if not tweet_records:
            return None
        
        earliest_tweet = None
        earliest_time = None
        
        for record in tweet_records:
            created_at = record["created_at"]
            if isinstance(created_at, str):
                created_at = DateTimeHandler.parse_twitter_time(created_at)
            
            if not created_at:
                continue
            
            if earliest_time is None or created_at < earliest_time:
                earliest_time = created_at
                earliest_tweet = record
        
        return earliest_tweet.get("user_name") if earliest_tweet else None

class TweetService:
    def __init__(self):
        self.api_client = TwitterAPIClient()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.tweet_processor = TweetProcessor(self.sentiment_analyzer)
        self.db_manager = DatabaseManager()
        self.wom_calculator = WOMCalculator()
    
    async def initialize(self):
        await self.sentiment_analyzer.initialize()
        logger.info("Tweet service initialized successfully")
    
    async def fetch_and_store_tweets_for_token_optimized(self, token_symbol: str) -> bool:
        try:
            logger.debug(f"Processing recent tweets for token: {token_symbol}")
            
            last_seen_tweet_id = await self.db_manager.get_last_seen_tweet_id(token_symbol)
            
            raw_tweets = await self.api_client.fetch_recent_tweets_optimized(token_symbol, last_seen_tweet_id)
            
            if not raw_tweets:
                logger.debug(f"No new tweets found for {token_symbol}")
                return False
            
            processed_tweets = await self.tweet_processor.process_tweets(raw_tweets, token_symbol)
            if not processed_tweets:
                logger.debug(f"No tweets passed processing for {token_symbol}")
                return False
            
            stored_count = await self.db_manager.store_tweets(processed_tweets)
            logger.info(f"Stored {stored_count} new tweets for {token_symbol}")
            
            return stored_count > 0
            
        except Exception as e:
            logger.error(f"Failed to process tweets for {token_symbol}: {e}")
            return False
    
    async def fetch_and_store_tweets_for_token(self, token_symbol: str) -> bool:
        try:
            logger.info(f"Processing tweets for token: {token_symbol}")
            
            raw_tweets = await self._fetch_all_recent_tweets(token_symbol)
            if not raw_tweets:
                logger.info(f"No recent tweets found for {token_symbol}")
                return False
            
            processed_tweets = await self.tweet_processor.process_tweets(raw_tweets, token_symbol)
            if not processed_tweets:
                logger.info(f"No tweets passed processing for {token_symbol}")
                return False
            
            existing_ids = await self.db_manager.get_existing_tweet_ids(token_symbol)
            new_tweets = [
                tweet for tweet in processed_tweets 
                if tweet.tweet_id not in existing_ids
            ]
            
            if not new_tweets:
                logger.info(f"No new tweets to store for {token_symbol}")
                return False
            
            stored_count = await self.db_manager.store_tweets(new_tweets)
            logger.info(f"Stored {stored_count} new tweets for {token_symbol}")
            
            return stored_count > 0
            
        except Exception as e:
            logger.error(f"Failed to process tweets for {token_symbol}: {e}")
            return False
    
    async def _fetch_all_recent_tweets(self, token_symbol: str) -> List[RawTweet]:
        all_tweets = []
        seen_ids = set()
        seen_cursors = set()
        cursor = None
        pages = 0
        
        while pages < Config.MAX_FETCH_PAGES and len(all_tweets) < Config.MAX_TWEETS_PER_FETCH:
            try:
                if cursor and cursor in seen_cursors:
                    logger.warning(f"Cursor cycle detected for {token_symbol}, breaking")
                    break
                
                if cursor:
                    seen_cursors.add(cursor)
                
                page_tweets, next_cursor = await self.api_client.fetch_tweets(token_symbol, cursor)
                
                if not page_tweets:
                    logger.info(f"No more tweets available for {token_symbol}")
                    break
                
                recent_tweets = []
                for tweet in page_tweets:
                    if tweet.tweet_id in seen_ids:
                        continue
                    
                    if not DateTimeHandler.is_within_hours(tweet.created_at, Config.TWEET_TIME_WINDOW_HOURS):
                        continue
                    
                    seen_ids.add(tweet.tweet_id)
                    recent_tweets.append(tweet)
                
                all_tweets.extend(recent_tweets)
                pages += 1
                
                logger.info(f"[{token_symbol}] Page {pages}: {len(recent_tweets)} recent tweets (total: {len(all_tweets)})")
                
                if not next_cursor:
                    break
                
                cursor = next_cursor
                
                if len(seen_ids) > Config.MAX_MEMORY_CACHE_SIZE:
                    logger.warning(f"Clearing seen_ids cache for {token_symbol} (size: {len(seen_ids)})")
                    seen_ids.clear()
                
            except Exception as e:
                logger.error(f"Error fetching page {pages + 1} for {token_symbol}: {e}")
                break
        
        logger.info(f"Fetched {len(all_tweets)} recent tweets for {token_symbol} across {pages} pages")
        return all_tweets
    
    async def recalculate_wom_scores(self):
        try:
            active_tokens = await self.db_manager.get_active_tokens()
            if not active_tokens:
                logger.warning("No active tokens found for WOM score calculation")
                return
            
            logger.info(f"Recalculating WOM scores for {len(active_tokens)} tokens")
            
            for token in active_tokens:
                try:
                    await self._recalculate_token_wom_score(token)
                except Exception as e:
                    logger.error(f"Failed to recalculate WOM score for {token}: {e}")
                    continue
            
            logger.info("Completed WOM score recalculation for all tokens")
            
        except Exception as e:
            logger.error(f"Failed to recalculate WOM scores: {e}")
            raise ServiceError(f"WOM score recalculation failed: {e}")
    
    async def _recalculate_token_wom_score(self, token_symbol: str):
        try:
            recent_tweets = await self.db_manager.get_recent_tweets(token_symbol)
            
            if not recent_tweets:
                logger.debug(f"No recent tweets found for {token_symbol}, skipping WOM update")
                return
            
            wom_score = self.wom_calculator.calculate_final_wom_score(recent_tweets)
            avg_followers = self.wom_calculator.calculate_average_followers(recent_tweets)
            
            first_spotted_by = None
            token_info = await self._get_token_info(token_symbol)
            if token_info and not token_info.get("first_spotted_by"):
                age_str = token_info.get("age", "")
                if age_str and not age_str.endswith('d'):
                    first_spotted_by = self.wom_calculator.find_first_spotted_by(recent_tweets)
            
            await self.db_manager.update_token_metrics(
                token_symbol=token_symbol,
                wom_score=wom_score,
                tweet_count=len(recent_tweets),
                avg_followers=avg_followers,
                first_spotted_by=first_spotted_by
            )
            
            logger.debug(f"Updated {token_symbol}: WOM={wom_score}, tweets={len(recent_tweets)}, avg_followers={avg_followers}")
            
        except Exception as e:
            logger.error(f"Failed to recalculate WOM score for {token_symbol}: {e}")
            raise
    
    async def _get_token_info(self, token_symbol: str) -> Optional[Dict[str, Any]]:
        try:
            if not token_symbol.startswith('$'):
                token_symbol = f'${token_symbol}'
                
            query = tokens.select().where(tokens.c.token_symbol == token_symbol)
            result = await database.fetch_one(query)
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Failed to get token info for {token_symbol}: {e}")
            return None
    
    async def run_tweet_pipeline(self):
        try:
            logger.info("Starting tweet pipeline...")
            
            active_tokens = await self.db_manager.get_active_tokens()
            if not active_tokens:
                logger.warning("No active tokens found")
                return
            
            logger.info(f"Processing {len(active_tokens)} active tokens")
            
            results = await asyncio.gather(
                *(self.fetch_and_store_tweets_for_token(token) for token in active_tokens),
                return_exceptions=True
            )
            
            successful = 0
            failed = 0
            
            for token, result in zip(active_tokens, results):
                if isinstance(result, Exception):
                    logger.error(f"Pipeline failed for {token}: {result}")
                    failed += 1
                elif result:
                    logger.info(f"Pipeline completed successfully for {token}")
                    successful += 1
                else:
                    logger.info(f"Pipeline completed with no new data for {token}")
                    successful += 1
            
            logger.info(f"Tweet pipeline completed: {successful} successful, {failed} failed")
            
        except Exception as e:
            logger.error(f"Tweet pipeline failed: {e}")
            raise ServiceError(f"Tweet pipeline failed: {e}")
    
    async def run_tweet_pipeline_optimized(self):
        try:
            logger.info("Starting optimized tweet pipeline...")
            
            active_tokens = await self.db_manager.get_active_tokens()
            if not active_tokens:
                logger.warning("No active tokens found")
                return
            
            logger.info(f"Processing {len(active_tokens)} active tokens (optimized)")
            
            results = await asyncio.gather(
                *(self.fetch_and_store_tweets_for_token_optimized(token) for token in active_tokens),
                return_exceptions=True
            )
            
            successful = 0
            failed = 0
            new_tweets_found = 0
            
            for token, result in zip(active_tokens, results):
                if isinstance(result, Exception):
                    logger.error(f"Optimized pipeline failed for {token}: {result}")
                    failed += 1
                elif result:
                    logger.debug(f"Optimized pipeline found new tweets for {token}")
                    new_tweets_found += 1
                    successful += 1
                else:
                    logger.debug(f"Optimized pipeline: no new tweets for {token}")
                    successful += 1
            
            logger.info(f"Optimized tweet pipeline completed: {successful} successful, {failed} failed, {new_tweets_found} tokens with new tweets")
            
        except Exception as e:
            logger.error(f"Optimized tweet pipeline failed: {e}")
            raise ServiceError(f"Optimized tweet pipeline failed: {e}")
    
    async def run_score_pipeline(self):
        try:
            logger.info("Starting score calculation pipeline...")
            await self.recalculate_wom_scores()
            logger.info("Score calculation pipeline completed")
        except Exception as e:
            logger.error(f"Score pipeline failed: {e}")
            raise ServiceError(f"Score pipeline failed: {e}")
    
    async def run_maintenance(self):
        try:
            logger.info("Running maintenance tasks...")
            await self.db_manager.prune_old_tweets()
            logger.info("Maintenance completed")
        except Exception as e:
            logger.error(f"Maintenance failed: {e}")
            raise ServiceError(f"Maintenance failed: {e}")
    
    async def run_full_pipeline(self):
        try:
            logger.info("Starting full pipeline...")
            
            await self.initialize()
            
            await self.run_tweet_pipeline()
            await self.run_score_pipeline()
            await self.run_maintenance()
            
            logger.info("Full pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Full pipeline failed: {e}")
            raise ServiceError(f"Full pipeline failed: {e}")
    
    async def initialize_new_token(self, token_symbol: str) -> bool:
        try:
            logger.info(f"Initializing new token {token_symbol} with full history...")
            success = await self.fetch_and_store_tweets_for_token(token_symbol)
            if success:
                logger.info(f"Successfully initialized {token_symbol}")
                await self._recalculate_token_wom_score(token_symbol)
            else:
                logger.warning(f"❌ Failed to initialize {token_symbol}")
            return success
        except Exception as e:
            logger.error(f"Error initializing token {token_symbol}: {e}")
            return False

_service_instance = None

async def get_service() -> TweetService:
    global _service_instance
    if _service_instance is None:
        _service_instance = TweetService()
        await _service_instance.initialize()
    return _service_instance

async def fetch_active_tokens() -> List[str]:
    service = await get_service()
    return await service.db_manager.get_active_tokens()

async def fetch_tweets_from_rapidapi(token_symbol: str, cursor: Optional[str] = None, retries: int = 3) -> Tuple[List[Dict], Optional[str]]:
    service = await get_service()
    try:
        raw_tweets, next_cursor = await service.api_client.fetch_tweets(token_symbol, cursor)
        tweet_dicts = []
        for tweet in raw_tweets:
            tweet_dict = {
                "tweet_id": tweet.tweet_id,
                "text": tweet.text,
                "created_at": tweet.created_at.strftime("%a %b %d %H:%M:%S %z %Y"),
                "user_info": {
                    "screen_name": tweet.user_info.screen_name,
                    "followers_count": tweet.user_info.followers_count,
                    "avatar": tweet.user_info.avatar
                },
                "type": "tweet"
            }
            tweet_dicts.append(tweet_dict)
        return tweet_dicts, next_cursor
    except (APIError, ValidationError) as e:
        logger.error(f"API fetch failed: {e}")
        return [], None

async def run_tweet_pipeline():
    service = await get_service()
    await service.run_tweet_pipeline()

async def run_score_pipeline():
    service = await get_service()
    await service.run_score_pipeline()

async def prune_old_tweets():
    service = await get_service()
    await service.run_maintenance()

async def tweet_score_deactivate_pipeline_optimized(tweet_service: TweetService):
    if not tweet_service:
        logger.warning("Tweet service not available, skipping tweet pipeline")
        return
    
    try:
        from services.token_service import deactivate_low_activity_tokens
        
        await tweet_service.run_tweet_pipeline_optimized()
        await tweet_service.run_score_pipeline()
        
        await deactivate_low_activity_tokens()
        
    except Exception as e:
        logger.error(f"Optimized tweet pipeline failed: {e}")

async def initialize_new_token(token_symbol: str, tweet_service: TweetService = None) -> bool:
    if tweet_service is None:
        tweet_service = await get_service()
    
    return await tweet_service.initialize_new_token(token_symbol)