from fastapi import FastAPI
import pandas as pd
import os
from dotenv import load_dotenv
from new_pairs_tracker import get_filtered_pairs
from twitter_analysis import analyze_cashtags
import logging

# Configure logging with a professional format
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)

# Load environment variables
load_dotenv()

# Ensure API token is available
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")

# Initialize FastAPI app
app = FastAPI()

def fetch_and_analyze():
    """
    Fetch filtered token pairs and analyze their sentiment.
    Returns:
        pd.DataFrame: Dataframe with sentiment analysis results.
    """
    logging.info("Fetching filtered token pairs...")
    tokens = get_filtered_pairs()

    if not tokens:
        logging.info("No tokens found matching the criteria.")
        return pd.DataFrame()


    cashtags = [token['token_symbol'] for token in tokens]
    logging.info(f"Tokens for sentiment analysis: {cashtags}")

    if not cashtags:
        logging.info("No valid cashtags to analyze.")
        return pd.DataFrame()

    logging.info("Performing sentiment analysis...")
    results_df = analyze_cashtags(cashtags)
    
    return results_df

@app.get("/tokens")
def get_token_sentiment():
    """
    API Endpoint to fetch and return token sentiment data.
    Returns:
        JSON response with token sentiment analysis results.
    """
    results_df = fetch_and_analyze()
    
    if results_df.empty:
        logging.info("No data available")
        return {"message": "No data available"}
    
    logging.info("Token Sentiment Analysis Results:")
    logging.info(results_df)
    
    return results_df.to_dict(orient="records")

if __name__ == "__main__":
    # For testing without uvicorn, simply call the analysis function and print results
    results = fetch_and_analyze()
    logging.info("Final results:")
    logging.info(results)
    print("Final results:")
    print(results)
