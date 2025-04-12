import os
from dotenv import load_dotenv
from sqlalchemy import MetaData
from databases import Database

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL") 

# Async database instance
database = Database(DATABASE_URL)

# SQLAlchemy metadata
sa_metadata = MetaData()
