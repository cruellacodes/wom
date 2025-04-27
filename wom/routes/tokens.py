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
        # Step 1: Try to find token in DB
        query = tokens.select().where(tokens.c.address == token_address.lower())
        result = await database.fetch_one(query)

        if result:
            # Found in database
            return {
                "token_symbol": result["token_symbol"],
                "token_name": result["token_name"],
                "address": result["address"],
                "marketCap": result["market_cap_usd"],
                "volume24h": result["volume_usd"],
                "liquidity": result["liquidity_usd"],
                "priceChange1h": result["pricechange1h"],
                "dexUrl": result["dex_url"],
                "ageHours": result["age_hours"],
                "wom_score": result["wom_score"]
            }

        # Step 2: Not found, fetch from Dexscreener
        dexscreener_url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"

        async with httpx.AsyncClient() as client:
            response = await client.get(dexscreener_url)
            response.raise_for_status()
            data = response.json()

        # Validate API data
        pairs = data.get("pairs", [])
        if not pairs:
            raise HTTPException(status_code=404, detail="Token not found")

        # Pick the first pair (most popular)
        pair = pairs[0]

        return {
            
            "symbol": pair["baseToken"]["symbol"],
            "token_name": pair["baseToken"]["name"],
            "address": pair["baseToken"]["address"],
            "marketCap": pair.get("fdv", 0),
            "volume24h": pair.get("volume", {}).get("h24", 0),
            "liquidity": pair.get("liquidity", {}).get("usd", 0),
            "priceChange1h": pair.get("priceChange", {}).get("h1", 0),
            "dexUrl": f"https://dexscreener.com/{pair['chainId']}/{pair['pairAddress']}",
            "ageHours": None 
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching token: {str(e)}")