import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db import database
from services.token_service import (
    fetch_tokens,
    deactivate_low_activity_tokens,
    delete_old_tokens,
)

from services.tweet_service import TweetService, ServiceError, tweet_score_deactivate_pipeline_optimized
from routes.tokens import tokens_router
from routes.tweets import tweets_router

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s", level=logging.INFO
)

def make_loop(fn, interval_seconds):
    """Spawn a background task that runs `await fn()` every interval_seconds."""
    async def _loop():
        logging.info(f"Loop started: {fn.__name__} every {interval_seconds}s")
        while True:
            start = datetime.now(timezone.utc)
            try:
                await fn()
            except Exception as exc:
                logging.error(f"[{fn.__name__}] Error: {exc}", exc_info=True)
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            logging.debug(f"[{fn.__name__}] Took {elapsed:.2f}s")
            await asyncio.sleep(max(0, interval_seconds - elapsed))
    task = asyncio.create_task(_loop())
    
    # crash monitoring
    def handle_crash(task):
        try:
            task.result()
        except asyncio.CancelledError:
            logging.info(f"[{fn.__name__}] Task was cancelled.")
        except Exception as e:
            logging.critical(f"[{fn.__name__}] Task crashed fatally: {e}", exc_info=True)

    task.add_done_callback(handle_crash)
    return task


@asynccontextmanager
async def lifespan(app: FastAPI):
    from services.search_service import process_search_queue 
    from services.volume_service import process_volume_queue 

    # Connect to DB
    await database.connect()
    logging.info("Connected to database.")

    tweet_service = TweetService()
    try:
        await tweet_service.initialize()
        app.state.tweet_service = tweet_service
        logging.info("Tweet service initialized successfully.")
    except ServiceError as e:
        logging.error(f"Failed to initialize tweet service: {e}")
        app.state.tweet_service = None

    # Create in-memory async queues
    search_queue = asyncio.Queue(maxsize=100)  
    app.state.search_queue = search_queue
    
    volume_queue = asyncio.Queue(maxsize=50)
    app.state.volume_queue = volume_queue

    # background workers
    tasks = [
        make_loop(fetch_tokens, 1800), 
        make_loop(lambda: tweet_score_deactivate_pipeline_optimized(app.state.tweet_service), 120), # 2 minutes
        make_loop(lambda: maintenance_pipeline(app.state.tweet_service), 1800),  # 30 minutes
    ]

    # Launch 5 parallel search processors
    for i in range(5):
        tasks.append(make_loop(lambda: process_search_queue(search_queue), 1))
    
    # Launch 3 parallel volume processors
    for i in range(3):
        tasks.append(make_loop(lambda: process_volume_queue(volume_queue), 1))

    yield

    logging.info("Shutting down background tasks…")
    for t in tasks:
        t.cancel()
    
    # Wait for tasks to finish gracefully
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10.0)
    except asyncio.TimeoutError:
        logging.warning("Some background tasks didn't finish gracefully within timeout")
    
    await database.disconnect()
    logging.info("Disconnected from database.")

app = FastAPI(lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],   
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    """Enhanced health check that includes service status"""
    tweet_service_status = "ok" if hasattr(app.state, 'tweet_service') and app.state.tweet_service else "unavailable"
    return {
        "status": "ok", 
        "timestamp": datetime.utcnow().isoformat(),
        "services": {
            "tweet_service": tweet_service_status,
            "database": "connected" if database.is_connected else "disconnected"
        }
    }

async def tweet_score_deactivate_pipeline(tweet_service: TweetService):
    """Main pipeline for tweets, scoring, and token deactivation"""
    if not tweet_service:
        logging.warning("Tweet service not available, skipping tweet pipeline")
        return
    
    try:
        # Run tweet fetching and scoring
        await tweet_service.run_tweet_pipeline()
        await tweet_service.run_score_pipeline()
        
        # Run token deactivation
        await deactivate_low_activity_tokens()
        
    except ServiceError as e:
        logging.error(f"Tweet pipeline failed: {e}")

    except Exception as e:
        logging.error(f"Unexpected error in tweet pipeline: {e}", exc_info=True)

async def maintenance_pipeline(tweet_service: TweetService):
    """Maintenance tasks pipeline"""
    if not tweet_service:
        logging.warning("Tweet service not available, skipping maintenance")
        return
    
    try:
        # Run tweet service maintenance
        await tweet_service.run_maintenance()
        
        # Run token cleanup 
        await delete_old_tokens()
        
    except ServiceError as e:
        logging.error(f"Maintenance pipeline failed: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in maintenance pipeline: {e}", exc_info=True)

# Include routers
app.include_router(tokens_router)
app.include_router(tweets_router)

# === Error handlers ===

@app.exception_handler(ServiceError)
async def service_error_handler(request, exc):
    """Handle service errors gracefully"""
    logging.error(f"Service error: {exc}")
    return {"error": "Service temporarily unavailable", "detail": str(exc)}