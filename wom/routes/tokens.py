from fastapi import APIRouter, HTTPException, Query # type: ignore
from sqlalchemy import select, func # type: ignore
from models import tokens
from db import database
from services.token_service import fetch_token_info_by_address, format_token_age
from pydantic import BaseModel
from services.token_service import store_tokens
from services.tweet_service import run_tweet_pipeline, run_score_pipeline

tokens_router = APIRouter()

@tokens_router.get("/tokens")
async def get_tokens(
    page: int = Query(1, gt=0),
    limit: int = Query(20, le=100),
    only_active: bool = Query(False),
):
    offset = (page - 1) * limit

    base_query = tokens.select()
    if only_active:
        base_query = base_query.where(tokens.c.is_active == True)

    data_query = base_query.limit(limit).offset(offset)
    count_query = select(func.count()).select_from(base_query.alias("sub"))

    rows = await database.fetch_all(data_query)
    total = await database.fetch_val(count_query)

    return {
        "tokens": [dict(row) for row in rows],
        "page": page,
        "totalPages": (total + limit - 1) // limit,
        "totalCount": total,
    }

@tokens_router.get("/search-token/{token_address}")
async def search_token(token_address: str):
    token_info = await fetch_token_info_by_address(token_address.lower())

    if not token_info or not isinstance(token_info, dict):
        raise HTTPException(status_code=404, detail="Token not found on Dexscreener.")

    base = token_info.get("baseToken", {})
    pair_created_at = token_info.get("pairCreatedAt")

    return {
        "symbol": base.get("symbol"),
        "token_name": base.get("name"),
        "address": base.get("address"),
        "marketCap": token_info.get("marketCap", 0),
        "volume24h": token_info.get("volume", {}).get("h24", 0),
        "liquidity": token_info.get("liquidity", {}).get("usd", 0),
        "priceUsd": token_info.get("priceUsd"),
        "priceChange1h": token_info.get("priceChange", {}).get("h1", 0),
        "dexUrl": token_info.get("url", "#"),
        "imageUrl": token_info.get("info", {}).get("imageUrl"),
        "age": format_token_age(pair_created_at) if pair_created_at else "N/A",
    }

class TokenAddressList(BaseModel):
    addresses: list[str]

@tokens_router.post("/add-manual-tokens")
async def add_manual_tokens(payload: TokenAddressList):
    added_tokens = []
    failed_tokens = []

    for address in payload.addresses:
        try:
            token_info = await fetch_token_info_by_address(address.lower())

            if not token_info or not isinstance(token_info, dict):
                failed_tokens.append({"address": address, "reason": "Token not found"})
                continue

            base = token_info.get("baseToken", {})
            liq = token_info.get("liquidity", {}).get("usd", 0)
            mcap = token_info.get("marketCap", 0)
            vol = token_info.get("volume", {}).get("h24", 0)


            token_data = [{
                "token_symbol": f"${base.get('symbol', '').strip().lower()}",
                "token_name": base.get("name", "unknown"),
                "image_url": token_info.get("info", {}).get("imageUrl", None),
                "address": base.get("address", "N/A"),
                "volume_usd": vol,
                "liquidity_usd": liq,
                "market_cap_usd": mcap,
                "priceChange1h": token_info.get("priceChange", {}).get("h1", 0),
                "age": format_token_age(token_info.get("pairCreatedAt", 0)) if token_info.get("pairCreatedAt") else "manual",
                "launchpad": "believe"
            }]

            await store_tokens(token_data)
            added_tokens.append(base.get("symbol", address))

        except Exception as e:
            failed_tokens.append({"address": address, "reason": str(e)})

    await run_tweet_pipeline()
    await run_score_pipeline()

    return {
        "added": added_tokens,
        "failed": failed_tokens,
        "message": f"Processed {len(payload.addresses)} tokens"
    }