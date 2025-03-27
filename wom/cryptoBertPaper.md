# **CryptoBERT: AI-Powered Sentiment Analysis in Crypto Markets**

## **Abstract**
This document outlines the integration of **CryptoBERT**, a deep learning-based NLP model, into a cryptocurrency sentiment analysis system. By leveraging transformer architecture and crypto-specific training data, CryptoBERT provides real-time sentiment scores for tweets mentioning crypto tokens. This enables a structured, AI-driven approach to understanding market sentiment at both micro (tweet) and macro (token) levels.

---

## **1. Introduction**
Crypto markets move at the speed of the internet. Price action is increasingly dictated by community chatter, memes, and hype across platforms like Twitter. However, traditional sentiment analysis tools fail to interpret the fast-changing and often chaotic language of crypto culture.

**CryptoBERT** addresses this gap by providing a **domain-specific sentiment classifier**, trained on millions of crypto-native tweets. It understands abbreviations ("LFG", "HODL"), emojis, slang, and token tags (like `$DOGE` or `#Solana`), enabling precise classification of sentiment polarity in crypto discussions.

---

## **2. AI-Powered Sentiment Modeling with CryptoBERT**

### **2.1. Model Architecture**
CryptoBERT is a **transformer-based model** fine-tuned from **BERTweet-base** (developed by VinAI). It inherits the language modeling strength of Google’s BERT, adapted specifically for **social media text**.

- **Pretrained on tweets**
- **Fine-tuned on 3.2M crypto-tagged tweets**
- Classifies tweets into: **Bearish**, **Neutral**, or **Bullish**

---

### **2.2. How the Classification Works**
CryptoBERT uses a standard softmax layer to output probabilities for each class:

\[
P(s_i) = \frac{e^{z_i}}{\sum_{j=1}^{N} e^{z_j}}
\]

Where:
- \( P(s_i) \) = probability of sentiment class \( s_i \)
- \( z_i \) = raw logit from the model
- \( N \) = total number of classes (3)

### **2.3. Tweet-Level Sentiment Scoring**
To compute a numerical score from the softmax output, the system applies a **weighted sentiment scoring formula**:

```python
score = 0 * P_bearish + 1 * P_neutral + 2 * P_bullish
```

This score is normalized to the range **[0, 2]**:
- **0** = fully Bearish
- **1** = Neutral
- **2** = fully Bullish

#### **Example: Bullish Tweet**
> "$PEPE just flipped $DOGE in volume. This is insane! 🚀🔥"

Model output:
```json
[
  {"label": "Bearish", "score": 0.03},
  {"label": "Neutral", "score": 0.12},
  {"label": "Bullish", "score": 0.85}
]
```

Score = (0 × 0.03) + (1 × 0.12) + (2 × 0.85) = **1.82**

#### **Example: Bearish Tweet**
> "Looks like $BTC is heading for another crash. 👎"

Model output:
```json
[
  {"label": "Bearish", "score": 0.91},
  {"label": "Neutral", "score": 0.07},
  {"label": "Bullish", "score": 0.02}
]
```

Score = (0 × 0.91) + (1 × 0.07) + (2 × 0.02) = **0.11**

This method effectively maps sentiment into a numerical continuum without discarding bearish content. High bearish probability pushes the score closer to 0.

---

## **3. Integration in This Project**

### **3.1. Data Pipeline**
- Tweets mentioning crypto tokens are fetched in real time.
- Spam, bots, and non-English posts are filtered out.
- Each tweet is processed through the CryptoBERT pipeline.

```python
pipe = TextClassificationPipeline(model=model, tokenizer=tokenizer, return_all_scores=True)

async def analyze_sentiment(text):
    preds = pipe(text)[0]
    return round((1 * preds[1]['score']) + (2 * preds[2]['score']), 2)
```

### **3.2. Token-Level Aggregation (WOM Score)**
The **WOM Score** is calculated as the average score of all tweets for a token:

```python
avg_score = round((sum(tweet_scores) / len(tweet_scores)) / 2 * 100, 2)
```

- Score is converted to a **0–100 scale**
- Displayed on the frontend as a progress bar
- Enables visual sentiment comparison across tokens

Each tweet retains its individual `wom_score` to power micro-level scatter plots and sentiment bubbles.

---

## **4. Conclusion**
CryptoBERT enables real-time, automated understanding of market sentiment using deep learning. Its crypto-specific design ensures accurate classification of nuanced, noisy, and emotionally charged language common in DeFi and memecoin communities.

By converting social chatter into structured sentiment scores, this system brings clarity to chaos and empowers smarter decision-making in volatile markets.

---

## **References**
1. Vaswani et al. (2017), *Attention Is All You Need*, Google Research  
2. VinAI Research, *BERTweet-base*  
3. Hugging Face, *Transformers Documentation*  
4. ElKulako (2021), *CryptoBERT Model Card*