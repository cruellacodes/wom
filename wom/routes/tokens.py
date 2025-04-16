from fastapi import APIRouter
from services.token_service import fetch_tokens_from_db

tokens_router = APIRouter()

@tokens_router.get("/tokens")
async def get_tokens():
    result = await fetch_tokens_from_db()
    return result
