import asyncio
from token_service import fetch_token_info_by_pair_address  # adjust if needed

async def main():
    token = "fwavj2avyny3myrhrprytbhx51by4cc4jg9ttmblc1ra"
    result = await fetch_token_info_by_pair_address(token)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
