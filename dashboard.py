import streamlit as st
import pandas as pd
import asyncio
from crypto_lens.raydium_pool_tracker import detect_pools

st.title("Crypto Lens - Raydium Pool Detector")
st.write("Monitoring new Raydium pools with a market cap above $100,000...")

placeholder = st.empty()

if st.button("Start Monitoring"):
    async def start_monitoring():
        async for tokens in detect_pools():
            filtered_data = [
                {
                    "Token0": token[0],
                    "Market Cap (Token0)": f"${token[1]:,}",
                    "Token1": token[2],
                    "Market Cap (Token1)": f"${token[3]:,}",
                    "LP Pair Address": token[6],
                }
                for token in tokens
            ]

            if filtered_data:
                df = pd.DataFrame(filtered_data)
                placeholder.table(df)
            else:
                st.write("No new pairs with a market cap above $100,000 detected.")

    asyncio.run(start_monitoring())
