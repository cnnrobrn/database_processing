from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
import asyncio
from datetime import datetime
import os
from dotenv import load_dotenv
import requests

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

# Global variable to track the last time the database was checked
last_checked = datetime.utcnow()

async def poll_database():
    """
    Continuously polls the database for new records in the items table.
    """
    global last_checked
    while True:
        try:
            async with async_session() as session:
                # Query to find new records in the items table
                query = """
                SELECT id, Amazon_Search 
                FROM items 
                WHERE created_at > :last_checked
                """
                result = await session.execute(query, {"last_checked": last_checked})
                new_records = result.fetchall()

                # If new records are found, process them
                if new_records:
                    async with session.begin():
                        for record in new_records:
                            item_id, amazon_search = record
                            processed_search = oxy_search(amazon_search)

                            # Add new links to the database
                            for result in processed_search:
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

                    # Update the last_checked timestamp
                    last_checked = datetime.utcnow()

        except Exception as e:
            print(f"Error during database polling: {e}")

        # Sleep before the next polling cycle
        await asyncio.sleep(5)

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

def oxy_search(query):
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
            timeout=10
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

