from fastapi import APIRouter, HTTPException, Query # type: ignore
from sqlalchemy import select, func # type: ignore
from models import tokens
from db import database
from services.token_service import fetch_token_info_by_pair_address

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

@tokens_router.get("/search-token/{chain_id}/{token_address}")
async def search_token(chain_id: str, token_address: str):
    token_info = await fetch_token_info_by_pair_address(token_address.lower(), chain_id)

    if not token_info:
        raise HTTPException(status_code=404, detail="Token not found on Dexscreener.")

    base = token_info.get("baseToken", {})

    return {
        "symbol": base.get("symbol"),
        "token_name": base.get("name"),
        "address": base.get("address"),
        "marketCap": token_info.get("marketCap", 0),
        "volume24h": token_info.get("volume", {}).get("h24", 0),
        "liquidity": token_info.get("liquidity", {}).get("usd", 0),
        "priceUsd": token_info.get("priceUsd", None),
        "priceChange1h": token_info.get("priceChange", {}).get("h1", 0),
        "dexUrl": token_info.get("url", "#"),
        "ageHours": None,
    }
