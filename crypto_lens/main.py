import os
import pandas as pd
import subprocess
from dotenv import load_dotenv
from new_pairs_tracker import get_filtered_pairs
from twitter_analysis import analyze_cashtags

# Load environment variables
load_dotenv()

# Ensure API token is available
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")


def fetch_and_analyze():
    """
    Fetch filtered token pairs and analyze their sentiment.
    Returns:
        pd.DataFrame: Dataframe with sentiment analysis results.
    """
    # Step 1: Fetch filtered token pairs from DEX Screener
    print("🔄 Fetching filtered token pairs...")
    tokens = get_filtered_pairs()

    if not tokens:
        print("❌ No tokens found matching the criteria.")
        return None

    print(f"✅ Found {len(tokens)} tokens matching criteria.")

    # Step 2: Extract cashtags for sentiment analysis
    cashtags = [token['token_symbol'] for token in tokens]
    print(f"📊 Tokens for sentiment analysis: {cashtags}")

    if not cashtags:
        print("❌ No valid cashtags to analyze.")
        return None

    # Step 3: Perform sentiment analysis
    print("\n🔍 Performing sentiment analysis...")
    results_df = analyze_cashtags(cashtags)
    
    return results_df


def launch_dashboard(results_df):
    """
    Launch the Streamlit dashboard with the analyzed data.
    Args:
        results_df (pd.DataFrame): Dataframe containing analyzed results.
    """
    if results_df is None or results_df.empty:
        print("❌ No data to display in the dashboard.")
        return

    # Save the results to a temporary CSV file for the dashboard to read
    temp_file = "dashboard_data.csv"
    results_df.to_csv(temp_file, index=False)

    # Launch the dashboard with the updated data
    print("\n🚀 Launching the dashboard...")
    subprocess.run(["streamlit", "run", "dashboard.py", "--", temp_file])


def main():
    """
    Main entry point for fetching, analyzing, and visualizing sentiment.
    """
    # Step 1: Fetch and analyze tokens
    results_df = fetch_and_analyze()

    if results_df is not None and not results_df.empty:
        # Step 2: Launch the dashboard with analyzed data
        launch_dashboard(results_df)
    else:
        print("❌ No data available for visualization.")


if __name__ == "__main__":
    main()
