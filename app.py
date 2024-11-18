from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
import asyncio
from datetime import datetime
import os
from dotenv import load_dotenv
import requests
from contextlib import asynccontextmanager


# Load environment variables
load_dotenv()

# Railway database reference
DATABASE_URL = os.getenv('DATABASE_URL', '').replace("postgresql://", "postgresql+asyncpg://")
OXY_USERNAME = os.getenv("OXY_USERNAME")
OXY_PASSWORD = os.getenv("OXY_PASSWORD")

# FastAPI app instance
app = FastAPI()

# Database configuration
Base = declarative_base()
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Database Models
class Link(Base):
    __tablename__ = 'links'
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('items.id'), nullable=False)
    photo_url = Column(String, nullable=True)
    url = Column(String, nullable=False)
    price = Column(String, nullable=True)
    title = Column(String, nullable=False)
    rating = Column(Float, nullable=True)
    reviews_count = Column(Integer, nullable=True)
    merchant_name = Column(String, nullable=True)

@asynccontextmanager
async def get_session():
    """
    Provides a new `AsyncSession` for database operations.
    Ensures proper session lifecycle management.
    """
    async with async_session() as session:
        print(f"Transaction state: {session.in_transaction()}")
        print(f"Session ID: {id(session)}")
        try:
            yield session
        except Exception as e:
            print(f"Error in session: {e}")
            raise

# Global variable to track the last time the database was checked
last_checked_id = 0  # Start from 0 or the last processed `id`

async def poll_database():
    """
    Continuously polls the database for new records in the `items` table.
    """
    global last_checked_id
    last_checked_id = await get_last_checked_id()  # Initialize from `links` table or fallback to 0

    while True:
        try:
            # Create a new session and explicitly use the connection for reading
            async with get_session() as session:
                async with session.begin():
                    connection = await session.connection()
                    # Query to find new records in the `items` table
                    query = text("""
                    SELECT id, search 
                    FROM items 
                    WHERE id > :last_checked_id
                    """)
                    result = await connection.execute(query, {"last_checked_id": last_checked_id})
                    new_records = result.fetchall()
                    print(f"New records: {new_records}")

            # Process records if they exist
            if new_records:
                print('new records recieved')
                async with get_session() as session:
                    # Start a transaction for the write operation
                    async with session.begin():
                        for record in new_records:
                            item_id, search = record
                            processed_search = await oxy_search(search)

                            # Add new links to the `links` table
                            for result in processed_search:
                                print("adding link")
                                if not result.get('url'):
                                    continue
                                new_link = Link(
                                    item_id=item_id,
                                    url=result['url'],
                                    title=result['title'],
                                    photo_url=result['thumbnail'],
                                    price=result['price'],
                                    rating=result['rating'],
                                    reviews_count=result['reviews_count'],
                                    merchant_name=result['merchant_name']
                                )
                                session.add(new_link)
                                print("link added")

                # Update the last_checked_id to the highest `id` processed
                last_checked_id = max(record[0] for record in new_records)

        except Exception as e:
            print(f"Error during database polling: {e}")
        finally:
            # Sleep before the next polling cycle
            await asyncio.sleep(1)  # Adjust the interval as needed



@app.on_event("startup")
async def startup_event():
    """
    Starts the database polling task when the FastAPI app starts.
    """
    asyncio.create_task(poll_database())

@app.get("/")
async def root():
    """
    Root endpoint to verify that the service is running.
    """
    return {"message": "Polling the database for new records in the items table"}

async def oxy_search(query):
    """
    Sends a query to the Oxylabs API and returns structured search results.
    """
    payload = {
        'source': 'google_shopping_search',
        'domain': 'com',
        'pages': 1,
        'parse': True,
        'query': query
    }

    try:
        response = requests.post(
            'https://realtime.oxylabs.io/v1/queries',
            auth=(OXY_USERNAME, OXY_PASSWORD),
            json=payload,
            timeout=45
        )
        data = response.json()
        organic_results = data["results"][0]["content"]["results"]["organic"][:30]
        return [
            {
                "pos": result.get("pos"),
                "price": result.get("price"),
                "title": result.get("title"),
                "thumbnail": result.get("thumbnail"),
                "url": result.get("url"),
                "rating": result.get("rating"),
                "reviews_count": result.get("reviews_count"),
                "merchant_name": result.get("merchant", {}).get("name")
            }
            for result in organic_results
        ]
    except Exception as e:
        print(f"Error during Oxylabs API call: {e}")
        return []

async def get_last_checked_id():
    """
    Get the highest `item_id` from the `links` table.
    """
    print('get_last_checked_id')
    async with get_session() as session:
        try:
            query = text("SELECT COALESCE(MAX(item_id), 0) FROM links").execution_options(isolation_level="AUTOCOMMIT")
            result = await session.execute(query)
            return result.scalar()
        except Exception as e:
            print(f"Error fetching last_checked_id: {e}")
            return 0
