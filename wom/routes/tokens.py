from fastapi import APIRouter, Query
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
