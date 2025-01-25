import os
from dotenv import load_dotenv
from new_pairs_tracker import main as fetch_new_pairs
from twitter_analysis import analyze_cashtags
import pandas as pd
import subprocess

# Load environment variables
load_dotenv()
api_token = os.getenv("APIFY_API_TOKEN")
if not api_token:
    raise ValueError("API token not found in environment variables!")


def fetch_and_analyze():
    """Fetch new token pairs and analyze their sentiment."""
    print("Fetching new token pairs...")
    tokens = fetch_new_pairs()
    if not tokens:
        print("No new token pairs found.")
        return None

    # Extract and deduplicate cashtags
    cashtags = list(set(f"${token['token_symbol'].strip('$')}" for token in tokens if token.get("token_symbol")))
    print(f"Found unique cashtags: {cashtags}")

    if not cashtags:
        print("No valid cashtags to analyze.")
        return None

    # Perform sentiment analysis
    print("\nPerforming sentiment analysis...")
    results_df = analyze_cashtags(cashtags)
    return results_df


def launch_dashboard(results_df):
    """Launch the Streamlit dashboard with the analyzed data."""
    # Save the results to a temporary CSV file for the dashboard to read
    temp_file = "dashboard_data.csv"
    results_df.to_csv(temp_file, index=False)

    # Launch the dashboard, passing the temporary file as input
    print("\nLaunching the dashboard...")
    subprocess.run(["streamlit", "run", "dashboard.py", "--", temp_file])


def main():
    """Main entry point for fetching, analyzing, and visualizing sentiment."""
    results_df = fetch_and_analyze()

    if results_df is not None and not results_df.empty:
        # Launch the dashboard with the analyzed data
        launch_dashboard(results_df)
    else:
        print("No data to display in the dashboard.")


if __name__ == "__main__":
    main()
