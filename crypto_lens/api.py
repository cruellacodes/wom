import requests
from datetime import datetime, timedelta

def fetch_new_pairs():
    """
    Fetch new trading pairs from Raydium API launched in the last 24 hours with market cap > $100M.
    """
    url = "https://api-v3.raydium.io/pairs"
    
    try:
        # Make an API request to Raydium
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors
        pairs = response.json()  # Parse JSON response
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Raydium API: {e}")
        return []

    # Filter pairs based on launch date and market cap
    last_24_hours = datetime.utcnow() - timedelta(days=1)
    new_pairs = [
        pair for pair in pairs
        if datetime.strptime(pair.get('launch_date', '1970-01-01T00:00:00Z'), "%Y-%m-%dT%H:%M:%SZ") > last_24_hours
        and pair.get('market_cap', 0) > 100_000
    ]
    return new_pairs
