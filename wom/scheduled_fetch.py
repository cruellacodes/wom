import requests
import logging
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://word-of-mouth-xojl.onrender.com/run-scheduled-job"
SCHEDULE_KEY = os.getenv("SCHEDULE_KEY")

logging.basicConfig(level=logging.INFO)

try:
    logging.info("Triggering scheduled job via web service...")
    response = requests.get(API_URL, params={"key": SCHEDULE_KEY})

    if response.status_code == 200:
        logging.info(f"Success: {response.json()}")
    else:
        logging.error(f"Failed with status {response.status_code}: {response.text}")

except Exception as e:
    logging.error(f"Cron job trigger failed: {e}")
