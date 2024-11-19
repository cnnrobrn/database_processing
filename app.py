from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float, ForeignKey, select
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import asyncio
import os
from dotenv import load_dotenv
import httpx
from contextlib import asynccontextmanager
from typing import List, Optional
from datetime import datetime, timedelta

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
engine = create_async_engine(
    DATABASE_URL,
    echo=True,
    pool_size=20,  # Adjust based on your needs
    max_overflow=10,
    pool_timeout=30,
    pool_pre_ping=True  # Enable connection health checks
)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Database Models
class Item(Base):
    __tablename__ = 'items'
    id = Column(Integer, primary_key=True)
    outfit_id = Column(Integer, ForeignKey('outfits.id'), nullable=False)
    description = Column(String, nullable=True)
    search = Column(String, nullable=True)
    processed_at = Column(Float, nullable=True)  # New column to track processing
    links = relationship('Link', backref='item', lazy=True)

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
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()

async def process_item_batch(items: List[Item], session: AsyncSession):
    """Process a batch of items concurrently while maintaining order"""
    tasks = []
    for item in items:
        if not item.search:
            continue
        task = asyncio.create_task(oxy_search(item.search))
        tasks.append((item.id, task))
    
    for item_id, task in tasks:
        try:
            search_results = await task
            if search_results:
                # Add all links in a single transaction
                link_objects = []
                for result in search_results:
                    if not result.get('url'):
                        continue
                    link = Link(
                        item_id=item_id,
                        url=result['url'],
                        title=result['title'],
                        photo_url=result['thumbnail'],
                        price=result['price_str'],
                        rating=result['rating'],
                        reviews_count=result['reviews_count'],
                        merchant_name=result['merchant_name']
                    )
                    link_objects.append(link)
                
                if link_objects:
                    session.add_all(link_objects)
                    
                # Mark item as processed
                await session.execute(
                    text("UPDATE items SET processed_at = :now WHERE id = :item_id"),
                    {"now": datetime.utcnow().timestamp(), "item_id": item_id}
                )
        except Exception as e:
            print(f"Error processing item {item_id}: {e}")
            # Mark as failed by setting processed_at to a negative timestamp
            await session.execute(
                text("UPDATE items SET processed_at = :failed WHERE id = :item_id"),
                {"failed": -datetime.utcnow().timestamp(), "item_id": item_id}
            )

async def poll_database():
    """Poll database with improved reliability and batching"""
    batch_size = 5  # Adjust based on your needs
    
    while True:
        try:
            async with get_session() as session:
                async with session.begin():
                    # Get unprocessed items
                    query = text("""
                        SELECT id, search 
                        FROM items 
                        WHERE processed_at IS NULL
                        ORDER BY id
                        LIMIT :batch_size
                        FOR UPDATE SKIP LOCKED
                    """)
                    
                    result = await session.execute(query, {"batch_size": batch_size})
                    items = result.fetchall()
                    
                    if not items:
                        await asyncio.sleep(1)
                        continue
                    
                    await process_item_batch(items, session)
                    await session.commit()
                    
        except Exception as e:
            print(f"Error during database polling: {e}")
            await asyncio.sleep(1)

async def retry_failed_items():
    """Periodically retry failed items"""
    while True:
        try:
            async with get_session() as session:
                async with session.begin():
                    # Get items that failed processing (negative processed_at)
                    query = text("""
                        SELECT id, search 
                        FROM items 
                        WHERE processed_at < 0
                        ORDER BY id
                        LIMIT 5
                        FOR UPDATE SKIP LOCKED
                    """)
                    
                    result = await session.execute(query)
                    items = result.fetchall()
                    
                    if items:
                        await process_item_batch(items, session)
                        await session.commit()
                    
        except Exception as e:
            print(f"Error during retry: {e}")
        finally:
            await asyncio.sleep(60)  # Retry every minute

@app.on_event("startup")
async def startup_event():
    # Add the processed_at column if it doesn't exist
    async with engine.begin() as conn:
        await conn.execute(text("""
            DO $$ 
            BEGIN 
                IF NOT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name='items' AND column_name='processed_at'
                ) THEN 
                    ALTER TABLE items ADD COLUMN processed_at FLOAT;
                END IF;
            END $$;
        """))
    
    # Start both the main polling and retry tasks
    asyncio.create_task(poll_database())
    asyncio.create_task(retry_failed_items())

@app.get("/")
async def root():
    return {"message": "Polling the database for new records in the items table"}

async def oxy_search(query):
    """
    Sends a query to the Oxylabs API and returns structured search results.
    """
    print('oxy_search')
    payload = {
        'source': 'google_shopping_search',
        'domain': 'com',
        'pages': 1,
        'parse': True,
        'query': query
    }

    try:
        async with httpx.AsyncClient(timeout=45) as client:
            response = await client.post(
                'https://realtime.oxylabs.io/v1/queries',
                auth=(OXY_USERNAME, OXY_PASSWORD),
                json=payload
            )
            data = response.json()
            organic_results = data["results"][0]["content"]["results"]["organic"][:30]
            records = await asyncio.gather(
                *[process_result(result) for result in organic_results]
            )
            print(records)
            return records
    except Exception as e:
        print(f"Error during Oxylabs API call: {e}")
        return []

async def process_result(result):
    """
    Process a single result to extract necessary fields and get seller link.
    """
    product_id = result.get("product_id")
    seller_link = await get_seller_link(product_id) if product_id else None

    return {
        "pos": result.get("pos"),
        "price_str": result.get("price_str"),
        "title": result.get("title"),
        "thumbnail": result.get("thumbnail"),
        "url": seller_link,
        "rating": result.get("rating"),
        "reviews_count": result.get("reviews_count"),
        "merchant_name": result.get("merchant", {}).get("name"),
    }

async def get_last_checked_id():
    async with get_session() as session:
        try:
            query = text("SELECT COALESCE(MAX(item_id), 0) FROM links").execution_options(isolation_level="AUTOCOMMIT")
            result = await session.execute(query)
            return result.scalar()
        except Exception as e:
            print(f"Error fetching last_checked_id: {e}")
            return 0

async def get_seller_link(product_id):
    payload = {
        'source': 'google_shopping_product',
        'domain': 'com',
        'pages': 1,
        'parse': True,
        'query': product_id
    }
    print(f"Getting seller link for product_id: {product_id}")

    try:
        async with httpx.AsyncClient(timeout=45) as client:
            response = await client.post(
                'https://realtime.oxylabs.io/v1/queries',
                auth=(OXY_USERNAME, OXY_PASSWORD),
                json=payload
            )
            data = response.json()
            return get_first_seller_link(data)
    except Exception as e:
        print(f"Error during Oxylabs API call: {e}")
        return None

def get_first_seller_link(data):
    try:
        first_result = data["results"][0]
        first_online_pricing = first_result["content"]["pricing"]["online"][0]
        return first_online_pricing.get("seller_link")
    except (KeyError, IndexError):
        return None
