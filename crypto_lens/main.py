import os
from dotenv import load_dotenv
from new_pairs_tracker import get_filtered_pairs 
from twitter_analysis import analyze_cashtags
import pandas as pd
import subprocess

# Load environment variables
load_dotenv()
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("Apify API token not found in environment variables!")


def fetch_and_analyze():
    """
    Fetch filtered token pairs and analyze their sentiment.
    Returns:
        pd.DataFrame: Dataframe with sentiment analysis results.
    """
    # Step 1: Fetch filtered token pairs
    print("Fetching filtered token pairs...")
    tokens = get_filtered_pairs() 

    if not tokens:
        print("No tokens found matching the criteria.")
        return None

    print(f"Found {len(tokens)} tokens matching the criteria.")

    # Set cashtags to start sentiment analysis
    cashtags = list(token['token_symbol'] for token in tokens)
    print(f"Tokens to be analyzed: {cashtags}")
    
    if not cashtags:
        print("No valid cashtags to analyze.")
        return None

    # Step 2: Perform sentiment analysis
    print("\nPerforming sentiment analysis...")
    results_df = analyze_cashtags(cashtags)
    return results_df


def launch_dashboard(results_df):
    """
    Launch the Streamlit dashboard with the analyzed data.
    Args:
        results_df (pd.DataFrame): Dataframe containing analyzed results.
    """
    # Save the results to a temporary CSV file for the dashboard to read
    temp_file = "dashboard_data.csv"
    results_df.to_csv(temp_file, index=False)

    # Launch the dashboard, passing the temporary file as input
    print("\nLaunching the dashboard...")
    subprocess.run(["streamlit", "run", "dashboard.py", "--", temp_file])


def main():
    """
    Main entry point for fetching, analyzing, and visualizing sentiment.
    """
    # Fetch and analyze tokens
    results_df = fetch_and_analyze()

    if results_df is not None and not results_df.empty:
        # Launch the dashboard with the analyzed data
        launch_dashboard(results_df)
    else:
        print("No data to display in the dashboard.")


if __name__ == "__main__":
    main()
