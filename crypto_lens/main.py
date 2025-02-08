from fastapi import FastAPI, HTTPException
import os
from dotenv import load_dotenv
from new_pairs_tracker import get_filtered_pairs
from twitter_analysis import get_sentiment
import logging
from fastapi.middleware.cors import CORSMiddleware
import asyncio

# Configure logging
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)


# Load environment variables
load_dotenv()
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

app = FastAPI()

from fastapi.responses import Response

@app.get("/favicon.ico", include_in_schema=False)
async def ignore_favicon():
    return Response(status_code=204)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins (use ["http://localhost:5173"] for more security)
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

async def fetch_and_analyze():
    """
    Fetch filtered token pairs and merge with sentiment analysis results.
    
    Returns:
        List[dict]: Each dictionary contains:
            - Token
            - WomScore (sentiment percentage)
            - MarketCap
            - Age
            - Volume
            - MakerCount
            - Liquidity
    """
    logging.info("Fetching filtered token pairs...")
    tokens = await get_filtered_pairs()  # Expects a list of dicts with token details
    if not tokens:
        logging.info("No tokens found matching the criteria.")
        return []

    # Extract token symbols (cashtags) from tokens
    cashtags = [token['token_symbol'] for token in tokens]
    logging.info(f"Tokens for sentiment analysis: {cashtags}")

    # Get sentiment percentage for each token from twitter_analysis
    sentiment_dict = await get_sentiment(cashtags)
    
    # Merge the sentiment with token details.
    final_results = []
    for token in tokens:
        ts = token.get("token_symbol")
        wom_score = round(sentiment_dict.get(ts, 0) or 0, 2)

        wom_score = float(wom_score) if wom_score is not None else None

        result = {
            "Token": ts,
            "WomScore": wom_score,
            "MarketCap": token.get("market_cap_usd"),
            "Age": token.get("age_hours"),
            "Volume": token.get("volume_usd"),
            "MakerCount": token.get("maker_count"),
            "Liquidity": token.get("liquidity_usd"),
        }
        final_results.append(result)
    
    return final_results

@app.get("/tokens")
async def get_token_sentiment():
    """
    API Endpoint to fetch and return token information combined with sentiment analysis.
    
    Returns:
        JSON: List of token dictionaries.
    """
    try:
        results = await fetch_and_analyze()
        if not results:
            logging.info("No data available")
            return {"message": "No data available"}
        
        logging.info("Token Sentiment Analysis Results:")
        logging.info(results)
        return results
    except Exception as e:
        logging.error(f"Error in get_token_sentiment: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")