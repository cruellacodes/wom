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

# Custom CSS for terminal-style UI
# Custom CSS for full black background
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
    .css-18e3th9 {
        background-color: black;
        color: #00FF00;
    }
    .css-1d391kg {
        background-color: black;
        color: #00FF00;
    }
    .vega-tooltip {
        background-color: black !important; /* Black background */
        color: white !important; /* White text */
        font-family: "Courier New", monospace; /* Terminal font style */
        font-size: 14px; /* Adjust font size for readability */
        border: 1px solid #00FF33; /* Add a green border for terminal aesthetics */
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# Title and Header
st.title("💹 Crypto Sentiment Dashboard")
st.markdown("**_Visualizing bullishness for cryptocurrencies with a terminal-inspired style._**")

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

# Bar Chart of Bullishness
st.write("### Bullishness Bar Chart")
if not results_df.empty:
    bar_chart = (
        alt.Chart(results_df)
        .mark_bar()
        .encode(
            x=alt.X("Bullishness:Q", title="Bullishness (%)"),
            y=alt.Y("Cashtag:N", title="Cashtag", sort="-x"),
            color=alt.Color(
                "Bullishness:Q",
                scale=alt.Scale(scheme="viridis"),  # Use a color scheme
                legend=None,
            ),
            tooltip=["Cashtag", "Bullishness"],
        )
        .properties(
            width=800,  # Customize chart width
            height=400,  # Customize chart height
            background="black",
            title="Bullishness Scores by Cashtag",
        )
        .configure_axis(labelColor="#00FF00", titleColor="#00FF00")  # Terminal-style labels
        .configure_title(color="#00FF00", fontSize=16)  # Terminal-style title
    )
    st.altair_chart(bar_chart, use_container_width=True)
else:
    st.warning("No valid data for bar chart.")

# Timeline Chart: Bullishness Over Time
st.write("### Tweet Sentiment Over Time (Last 24 Hours)")
if not filtered_df.empty:
    tweet_timeline_chart = (
        alt.Chart(filtered_df)
        .mark_circle(size=80)  # Dots for tweets
        .encode(
            x=alt.X("created_at:T", title="Time (Last 24 Hours)"),  # X-axis: Timestamps
            y=alt.Y("Bullishness:Q", title="Bullishness (%)"),  # Y-axis: Bullishness
            color=alt.Color("Cashtag:N", legend=alt.Legend(title="Cashtag")),  # Different colors for cashtags
            tooltip=["Cashtag", "Bullishness", "created_at", "Tweet_Text"],  # Tooltip with tweet details
        )
        .properties(
            width=800,
            height=400,
            background="black",
            title="Bullishness Over Time (Last 24 Hours)",
        )
        .configure_axis(
            labelColor="#00FF00", titleColor="#00FF00"
        )  # Terminal-style labels
        .configure_title(
            color="#00FF00", fontSize=16
        )  # Terminal-style title
    )
    st.altair_chart(tweet_timeline_chart, use_container_width=True)
else:
    st.warning("No tweets found in the last 24 hours.")

# Full Results Table
st.write("### Full Results Table")
st.dataframe(
    results_df.style.set_properties(**{"background-color": "black", "color": "#00FF00"}),
    use_container_width=True,
)
