# WOM - Word of Mouth Intelligence Engine

[![Python](https://img.shields.io/badge/python-3.7%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**WOM** is an AI-powered analytics engine for extracting, processing, and scoring real-time social sentiment across newly launched crypto tokens. Designed to provide developers, analysts, and DeFi protocol operators with actionable intelligence on trending assets, WOM leverages a highly concurrent backend pipeline with deep integration into natural language sentiment modeling and dynamic scoring heuristics.


## ✶ Core Capabilities

* **Token Discovery Layer**:

  * Real-time ingestion of newly launched tokens
  * Symbol sanitization and semantic validation to prevent noise from malformed, irrelevant, or spoofed pairs.

* **Tweet Ingestion Pipeline**:

  * Token-aware social scraping via a high-availability request layer with pagination, deduplication, and cursor-based traversal.
  * Full-text tweet cleaning, timestamp parsing, and user quality scoring (based on follower counts, verified identities, etc).

* **Sentiment Analysis Engine**:

  * NLP-driven transformer classification over all relevant tweet content.
  * Softmax post-processing, noise suppression, and weighted WOM scoring.
  * Normalized per-tweet and per-token score aggregation using a time-weighted exponential decay function.

* **Token Intelligence Layer**:

  * Composite score (WOM Score) generation via Bayesian smoothing over decayed tweet sentiment.
  * Temporal pruning of stale tweets (older than 48h) and low-signal tokens (e.g. low liquidity, low market cap, low tweet velocity).
  * Dynamic updates of each token’s tweet count and average sentiment in the core PostgreSQL store.

## ✶ Tweet Service Deep Dive

* Tweets are fetched for each token from a social feed using hashtag and cashtag semantics.
* Each tweet is validated:

  * Skips non-tweets and invalid user profiles.
  * Parses timestamps to UTC, and drops tweets missing content.
* Tweets are scored for relevance using a simple `is_relevant_tweet` utility that avoids spam and noise.
* Tweets are then sent to an NLP classifier, which predicts bullish/neutral scores per tweet.
* A WOM score per tweet is computed using a linear combination of softmax scores.
* A final token-level WOM score is calculated with exponential decay + Bayesian smoothing.
* Only tweets from the last 48 hours are kept to ensure recency.


## ✶ Scoring Breakdown

**WOM Score** (0–100): A confidence-weighted metric representing token sentiment derived from recent social discourse.

* Final WOM = Bayesian average of weighted tweet scores, with decay based on tweet age.
* Tweets older than 48 hours are deleted during score recalculation to maintain score fidelity.


## ✶ Token Management

* **Storage**: New tokens are inserted or upserted into the database using `ON CONFLICT DO UPDATE` semantics.
* **Activity Decay Rules**:

  * Tokens older than 3h with <20 total tweets are deactivated.
  * Tokens older than 24h with <10 tweets in last 24h or low liquidity/volume are pruned.
  * Inactive tokens older than 5 days are permanently deleted along with their tweets.


## ✶ Public API

### `GET /tokens`

Returns all stored tokens with optional pagination and `only_active` filter.

### `GET /search-token/{token_address}`

Search for real-time data about a token by address from DEX metadata.

### `GET /tweets/{token_symbol}`

Live-fetch tweet data for a token and return current sentiment + recent tweets.

### `GET /stored-tweets?token_symbol=xyz`

Query all tweets currently stored in the DB for a token.


## ✶ Security & Performance

* Stateless service design (easily horizontally scalable)

* Pagination and cursor-based fetching to prevent rate abuse

* TTL-based tweet cleanup to manage DB bloat

* Retry + exponential backoff built into all external fetches

* Auto-deactivation of low-performing tokens

## ✶ Project Structure

wom/
├── services/
│   ├── token_service.py      # Token ingestion + filtering
│   ├── tweet_service.py      # Tweet ingestion, scoring, storage
├── models.py                 # DB schema (SQLAlchemy Core)
├── db.py                     # Async DB session setup
├── utils.py                  # Shared filters & text tools
├── main.py                   # FastAPI entrypoint
├── routers/
│   ├── tokens.py             # Token endpoints
│   ├── tweets.py             # Tweet endpoints

## ✶ Setup & Deployment

### Environment Requirements

* Python 3.11+
* PostgreSQL 13+
* Docker (recommended for deployment)
* NVIDIA GPU (optional but recommended for faster inference)

### Install & Boot

```bash
git clone https://github.com/<your-org>/wom
cd wom
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Docker Setup

```
# Build image
$ docker build -t wom-service .

# Run container
$ docker run -d -p 8000:8000 --env-file .env wom-service

```

## ✶ Final Notes

This system is designed for scalability, resilience, and real-time sentiment intelligence. It is battle-tested to support bursty token activity, concurrent ingestion, and accurate low-latency updates.

For advanced use cases such as alerting, price/sentiment correlation, or custom dashboards, integration with Kafka or Redis Streams is supported (WIP).

For contributions, fork the repo and submit a PR. For production deployment, containerization via Docker or Kubernetes is recommended for load-balanced environments.
