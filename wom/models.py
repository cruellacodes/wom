from sqlalchemy import Table, Column, Integer, String, Float, DateTime, Text
from db import sa_metadata
from datetime import datetime, timezone

default=lambda: datetime.now(timezone.utc)

tweets = Table(
    "tweets",
    sa_metadata,
    Column("id", String, primary_key=True),
    Column("token", String),
    Column("text", Text),
    Column("followers_count", Integer, default=0),
    Column("user_name", String),
    Column("profile_pic", String),
    Column("created_at", DateTime(timezone=True)),
    Column("wom_score", Float),
)

tokens = Table(
    "tokens",
    sa_metadata,
    Column("token_symbol", String, primary_key=True),
    Column("token_name", String),
    Column("address", String),
    Column("age_hours", Float),
    Column("volume_usd", Float),
    Column("maker_count", Integer),
    Column("liquidity_usd", Float),
    Column("market_cap_usd", Float),
    Column("dex_url", String),
    Column("pricechange1h", Float),
    Column("wom_score", Float),
    Column("tweet_count", Integer),
    Column("created_at", DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)),
)
