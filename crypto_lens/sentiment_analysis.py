import pandas as pd
import streamlit as st
import altair as alt
import sys
from datetime import datetime, timedelta
from pytz import utc

# Check if a data file is provided
data_file = None
if len(sys.argv) > 1:
    data_file = sys.argv[-1]

# Streamlit App Configuration
st.set_page_config(page_title="Crypto Sentiment Dashboard", layout="wide")

# Custom Terminal-Inspired CSS
st.markdown(
    """
    <style>
        body {
            background-color: black;
            color: #00FF00;
            font-family: "Courier New", monospace;
        }
        .stApp {
            background-color: black;
        }
        .stTextInput {
            text-align: center;
            font-size: 18px;
            font-weight: bold;
            background-color: black !important;
            color: #00FF00 !important;
            border: 2px solid #00FF00 !important;
            border-radius: 10px;
            text-align: center;
        }
        .css-18e3th9, .css-1d391kg {
            background-color: black;
            color: #00FF00;
        }
        .vega-tooltip {
            background-color: black !important;
            color: white !important;
            font-family: "Courier New", monospace;
            font-size: 14px;
            border: 1px solid #00FF33;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# Title & Header
st.markdown("<h1 style='text-align: center;'>💹 Crypto Sentiment Dashboard</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center;'>📊 Visualizing bullishness trends in crypto markets</h3>", unsafe_allow_html=True)

# User Token Input at the Center
st.markdown("<h4 style='text-align: center;'>🔍 Search for a Token</h4>", unsafe_allow_html=True)
token_input = st.text_input("", placeholder="Enter token symbol (e.g., BTC, ETH, SOL)", key="token_search")

# Load Data
if data_file:
    results_df = pd.read_csv(data_file)
else:
    st.error("No data provided for the dashboard.")
    st.stop()

# Filter for Last 24 Hours
current_time = datetime.utcnow().replace(tzinfo=utc)  # Make timezone-aware
last_24_hours = current_time - timedelta(hours=24)

# Convert `created_at` to datetime and filter
results_df["created_at"] = pd.to_datetime(results_df["created_at"], errors="coerce").dt.tz_convert(utc)
filtered_df = results_df[
    (results_df["created_at"] >= last_24_hours) & (results_df["created_at"] <= current_time)
]

# Display Trending Pairs Title
st.markdown("<h3 style='text-align: center;'>🔥 Hottest Trending Pairs in the Last 24 Hours</h3>", unsafe_allow_html=True)

# Bullishness Bar Chart
if not results_df.empty:
    bar_chart = (
        alt.Chart(results_df)
        .mark_bar()
        .encode(
            x=alt.X("Bullishness:Q", title="Bullishness (%)"),
            y=alt.Y("Cashtag:N", title="Cashtag", sort="-x"),
            color=alt.Color("Bullishness:Q", scale=alt.Scale(scheme="viridis"), legend=None),
            tooltip=["Cashtag", "Bullishness"],
        )
        .properties(
            width=800,
            height=400,
            background="black",
            title="Bullishness Scores by Cashtag",
        )
        .configure_axis(labelColor="#00FF00", titleColor="#00FF00")
        .configure_title(color="#00FF00", fontSize=16)
    )
    st.altair_chart(bar_chart, use_container_width=True)
else:
    st.warning("No valid data for bar chart.")

# Timeline Chart: Bullishness Over Time
st.markdown("<h3 style='text-align: center;'>📈 Tweet Sentiment Over Time (Last 24 Hours)</h3>", unsafe_allow_html=True)
if not filtered_df.empty:
    tweet_timeline_chart = (
        alt.Chart(filtered_df)
        .mark_circle(size=80)
        .encode(
            x=alt.X("created_at:T", title="Time (Last 24 Hours)"),
            y=alt.Y("Bullishness:Q", title="Bullishness (%)"),
            color=alt.Color("Cashtag:N", legend=alt.Legend(title="Cashtag")),
            tooltip=["Cashtag", "Bullishness", "created_at"],
        )
        .properties(
            width=800,
            height=400,
            background="black",
            title="Bullishness Over Time (Last 24 Hours)",
        )
        .configure_axis(labelColor="#00FF00", titleColor="#00FF00")
        .configure_title(color="#00FF00", fontSize=16)
    )
    st.altair_chart(tweet_timeline_chart, use_container_width=True)
else:
    st.warning("No tweets found in the last 24 hours.")
