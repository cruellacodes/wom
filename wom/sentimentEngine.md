# **WOM Sentiment Engine: Real-Time AI Signal for Token Hype**

## **Abstract**
This document details the architecture and methodology behind our custom-built **WOM Sentiment Engine** , an AI-powered system designed to analyze Twitter sentiment around crypto tokens in real time. 

By leveraging deep learning, time-sensitive weighting, and domain-specific linguistic understanding, the system computes a dynamic metric called the **WOM Score**. This score reflects both the emotional tone and the recency of conversations around each token, offering a transparent and scalable lens into community hype.

---

## **1. Introduction**
Crypto markets don’t move by logic. They move by memes, emotions, and viral narratives.

Twitter has become the town square of Web3 where new tokens go viral, reputations are built (or burned), and communities form before charts even exist. But traditional sentiment analysis tools don’t speak meme, can’t parse crypto slang, and definitely don’t understand the difference between “$PEPE is going to zero” and “$PEPE is going to the moon.”

That’s why we built the **WOM Sentiment Engine**: a system trained to understand the emotional heartbeat of the crypto space, one tweet at a time.

It:
- Detects sarcasm and FUD
- Parses slang, abbreviations, and token tags ($, #)

---

## **2. The Engine Behind the Score**

### **2.1. Sentiment Types and Classification**
Each tweet is classified into one of three sentiment types:
- **Bearish** — negative sentiment, FUD, panic, doubt.
- **Neutral** — news, stats, non-emotional statements.
- **Bullish** — excitement, hype, positivity, “moon talk.”

### **2.2. Classification Mechanics**
The engine analyzes tweet text and computes probabilities for each sentiment class:

$$
P(s_i) = \frac{e^{z_i}}{\sum_{j=1}^{3} e^{z_j}}
$$


Where:
- \( P(s_i) \) is the probability of sentiment class \( s_i \)
- \( z_i \) is the raw output (logit) from the classifier

We then calculate a continuous score per tweet using:

```python
score = 0 * P_bearish + 1 * P_neutral + 2 * P_bullish
```

This returns a **float in [0, 2]**, capturing sentiment with precision.

#### **Example 1: Bullish Tweet**
> "$PEPE just flipped $DOGE in volume. This is insane!"

```json
[
  {"label": "Bearish", "score": 0.03},
  {"label": "Neutral", "score": 0.12},
  {"label": "Bullish", "score": 0.85}
]
```
Final Score: **1.82**

#### **Example 2: Bearish Tweet**
> "Looks like $SOL is heading to 8 dollars."

```json
[
  {"label": "Bearish", "score": 0.91},
  {"label": "Neutral", "score": 0.07},
  {"label": "Bullish", "score": 0.02}
]
```
Final Score: **0.11**

---

## **3. From Tweets to WOM Score**
The **WOM Score** is a token-level metric calculated from the collective sentiment of all tweets about that token.

But we don’t just average tweets. We apply **two intelligent layers**:

### **3.1. Time Decay**
Recent tweets are more valuable than old ones. We apply exponential decay to each tweet’s score:
```python
decay_weight = math.exp(-age_in_hours / 12)
```
This ensures that **hype fades**, and only fresh conversations push the score higher.

### **3.2. Lifespan Normalization**
Tokens that have existed longer naturally accumulate more tweets. To prevent this from skewing results, we normalize based on the number of hours since the first tweet:
```python
lifespan_hours = max(1.0, (now - first_tweet_time).total_seconds() / 3600)
```
This makes new tokens competitive and **keeps the scoring field fair**.

### **3.3. Final WOM Score Formula**
```python
final_score = min(round((weighted_sum / lifespan_hours) / 2 * 100, 2), 100.0)
```
- **0** = Fully bearish
- **100** = Pure bullish momentum

Each token is updated live with its WOM Score and tweet volume.

---

## **4. Data Pipeline (Backend)**

### **4.1. Workflow Overview**
- RapidAPI fetches tweet timelines based on hashtags or cashtags (e.g. `$LLM`, `#fartcoin`)
- Tweets are filtered using:
  - Custom `is_relevant_tweet()` NLP logic
  - Follower thresholds (anti-bot)
  - Deduplication
- Sentiment scores are calculated and added to each tweet
- Tweets are stored in PostgreSQL via SQLAlchemy
- A batch score update runs per token

### **4.2. Example Code Snippet**
```python
async def analyze_sentiment(text):
    preds = pipeline(text)[0]
    return round((1 * preds[1]['score']) + (2 * preds[2]['score']), 2)
```

---

## **5. Use Cases & Visuals**
WOM powers:
- **Radar charts**: Top trending tokens by tweet count
- **Podium charts**: Top 3 tokens of the day
- **Sentiment heatmaps**: Tweet volume vs. bullishness
- **Scatter plots**: Tweet sentiment by follower count

It enables:
- **Traders** to catch alpha early
- **Founders** to validate buzz during launches
- **Analysts** to visualize market vibes

WOM Score = **the social layer of token discovery**.

---

## **6. Conclusion**
The WOM Sentiment Engine is built for the speed and chaos of crypto culture. It doesn’t just count tweets, it interprets mood, decodes sarcasm, and detects hype as it happens.

It is real-time, fair, explainable, and battle-tested.

We believe sentiment is alpha. WOM Score is how we capture it.