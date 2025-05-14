from sqlalchemy import Table, Column, String, Text, Integer, Float, Boolean, DateTime, ForeignKey, MetaData
from datetime import datetime, timezone

sa_metadata = MetaData()
default = lambda: datetime.now(timezone.utc)

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
    Column("last_seen_at", DateTime(timezone=True), default=default),
    Column("is_active", Boolean, default=True),
    Column("is_believe", Boolean, default=False),
)

tweets = Table(
    "tweets",
    sa_metadata,
    Column("tweet_id", String, primary_key=True), 
    Column("token_symbol", String, ForeignKey("tokens.token_symbol")),
    Column("text", Text),
    Column("followers_count", Integer, default=0),
    Column("user_name", String),
    Column("profile_pic", String),
    Column("created_at", DateTime(timezone=True)),
    Column("wom_score", Float),
    Column("tweet_url", String),
)