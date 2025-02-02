# Crypto Sentiment Analyzer

## Overview

**Crypto Sentiment Analyzer** is a robust pipeline designed to assess the market sentiment of cryptocurrency tokens using real-time Twitter data. The system performs the following steps:

1. **Token Fetching & Filtering:**  
   - Tokens are fetched from Dex Screener.
   - They are filtered based on criteria such as age, market cap, volume, maker count, and liquidity.

2. **Concurrent Tweet Fetching with Round-Robin Assignment:**  
   - For each filtered token, the latest tweets are fetched concurrently.
   - A pool of 10 Apify tasks is used to fetch tweets.
   - Tokens are assigned to these tasks using a **round-robin algorithm** that cycles through the list of available tasks sequentially. This ensures that the load is evenly distributed among the tasks.

3. **Sentiment Analysis using CryptoBERT:**  
   - Each tweet is preprocessed and analyzed using **CryptoBERT**, a transformer model fine-tuned exclusively on crypto-related data.
   - **Sentiment analysis** is the process of determining the emotional tone behind a piece of text—in this case, measuring how "bullish" tweets are regarding a specific token.
   - CryptoBERT has been trained on over **3.2 million crypto-related tweets**, enabling it to capture the nuances, slang, and context unique to cryptocurrency discussions.
   - The model computes a bullishness score for each tweet. These scores are aggregated using the median and then expressed as a percentage to represent the overall bullish sentiment for the token.

4. **Result Dispatch:**  
   - The aggregated sentiment for each token is sent to the frontend for display on dashboards or leaderboards.

> **Note:** A detailed pipeline diagram is included below.

## Pipeline Diagram

![Pipeline Diagram](architecture-diagram.png)

*The diagram above illustrates the complete flow—from token fetching and round-robin tweet retrieval to sentiment analysis and frontend integration.*

## Round-Robin Assignment Explained

- **What It Is:**  
  The round-robin algorithm cycles through a fixed list of tasks sequentially.
  
- **How It Works in This Pipeline:**  
  - Assume there are 10 task IDs and 25 tokens.
  - The first token is assigned to task ID 1, the second token to task ID 2, …, the tenth token to task ID 10.
  - The eleventh token is then assigned again to task ID 1, the twelfth to task ID 2, and so on.
  
- **Benefits:**  
  - **Load Balancing:** Ensures that no single task is overloaded.
  - **Concurrency:** Multiple tokens are processed in parallel, significantly speeding up the overall analysis.

## Sentiment Analysis with CryptoBERT

### What is Sentiment Analysis?

Sentiment analysis is a natural language processing (NLP) technique that determines the emotional tone behind a body of text. In our application, it measures how "bullish" (optimistic) the sentiment is towards a cryptocurrency token based on tweets.

### How CryptoBERT Works

- **Model Overview:**  
  CryptoBERT is a transformer-based model fine-tuned on over **3.2 million crypto-related tweets**. Its specialized training helps it understand the unique language and context found in cryptocurrency discussions.

- **Processing Steps:**
  1. **Preprocessing:**  
     Tweets are cleaned to remove URLs, extra spaces, and unwanted characters.
     
  2. **Tokenization:**  
     The cleaned text is tokenized using the model's `AutoTokenizer`.
     
  3. **Inference:**  
     The tokenized input is passed through `AutoModelForSequenceClassification`, generating raw logits for various sentiment classes.
     
  4. **Probability Conversion:**  
     The logits are converted into probabilities via the softmax function.
     
  5. **Bullishness Score Extraction:**  
     The probability corresponding to bullish sentiment (typically at index 2) is extracted as the bullishness score.
     
  6. **Aggregation:**  
     For each token, the individual bullishness scores from multiple tweets are aggregated using the median, and then multiplied by 100 to express the final sentiment as a percentage.

## Setup and Installation

### Prerequisites

- **Python 3.7+**
- Environment variables must be configured:
  - `APIFY_API_TOKEN`: Your Apify API token.
  - `TOKEN_ACTORS`: A comma-separated list of 10 Apify task IDs (or in the format `username~taskName`).
  - `COOKIES`: (Optional) Required cookies for Apify, if applicable.

### Installation Steps

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/yourusername/crypto-sentiment-analyzer.git
   cd crypto-sentiment-analyzer

2. **Create a Virtual Environment & Install Dependencies:**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt

3. **Run the Application:**

   ```bash
   python your_script.py
