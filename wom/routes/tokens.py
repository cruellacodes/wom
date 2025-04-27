from fastapi import APIRouter, HTTPException, Query
import httpx
from sqlalchemy import select, func
from models import tokens
from db import database

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
    try:
        # Step 1: Try from local database
        query = tokens.select().where(tokens.c.address == token_address.lower())
        result = await database.fetch_one(query)

        if result:
            return {
                "symbol": result["token_symbol"],
                "token_name": result["token_name"],
                "address": result["address"],
                "marketCap": result["market_cap_usd"],
                "volume24h": result["volume_usd"],
                "liquidity": result["liquidity_usd"],
                "priceUsd": None, 
                "priceChange1h": result["pricechange1h"],
                "dexUrl": result["dex_url"],
                "ageHours": result["age_hours"],
            }

        # Step 2: If not found, call Dexscreener API
        url = f"https://api.dexscreener.com/token-pairs/v1/{chain_id}/{token_address}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            pairs = response.json()

        if not pairs or not isinstance(pairs, list):
            raise HTTPException(status_code=404, detail="Token not found on Dexscreener.")

        # Pick the pair with the highest liquidity
        best_pair = max(pairs, key=lambda x: x.get("liquidity", {}).get("usd", 0))

        return {
            "symbol": best_pair["baseToken"]["symbol"],
            "token_name": best_pair["baseToken"]["name"],
            "address": best_pair["baseToken"]["address"],
            "marketCap": best_pair.get("marketCap", 0),
            "volume24h": best_pair.get("volume", {}).get("h24", 0),
            "liquidity": best_pair.get("liquidity", {}).get("usd", 0),
            "priceUsd": best_pair.get("priceUsd", None),
            "priceChange1h": best_pair.get("priceChange", {}).get("h1", 0),
            "dexUrl": best_pair.get("url", "#"),
            "ageHours": None  # still unknown unless we calculate manually
        }
    
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=404, detail="Token not found or Dexscreener error.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching token: {str(e)}")