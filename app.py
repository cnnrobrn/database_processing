from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float, ForeignKey, Boolean
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import asyncio
import os
from dotenv import load_dotenv
import httpx
from contextlib import asynccontextmanager
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
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Database Models
class Item(Base):
    __tablename__ = 'items'
    id = Column(Integer, primary_key=True)
    outfit_id = Column(Integer, ForeignKey('outfits.id'), nullable=False)
    description = Column(String, nullable=True)
    search = Column(String, nullable=True)
    is_processing = Column(Boolean, default=False)  # New column for locking
    processing_started = Column(Float, nullable=True)  # New column for timeout handling
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

async def claim_next_item():
    """Try to claim the next unprocessed item with proper locking"""
    async with get_session() as session:
        try:
            # Get and lock the next available item
            current_time = datetime.utcnow().timestamp()
            timeout_threshold = current_time - 300  # 5 minutes timeout

            query = text("""
                UPDATE items 
                SET is_processing = TRUE, processing_started = :current_time
                WHERE id = (
                    SELECT id 
                    FROM items 
                    WHERE (is_processing = FALSE OR processing_started < :timeout)
                    AND search IS NOT NULL
                    AND id NOT IN (SELECT DISTINCT item_id FROM links)
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, search;
            """)
            
            result = await session.execute(
                query, 
                {
                    "current_time": current_time,
                    "timeout": timeout_threshold
                }
            )
            await session.commit()
            
            record = result.first()
            return record if record else None

        except Exception as e:
            print(f"Error claiming item: {e}")
            await session.rollback()
            return None

async def release_item(item_id: int, success: bool = True):
    """Release the lock on an item"""
    async with get_session() as session:
        try:
            if success:
                # If successful, keep is_processing True to prevent reprocessing
                query = text("""
                    UPDATE items 
                    SET processing_started = NULL
                    WHERE id = :item_id
                """)
            else:
                # If failed, reset the lock so it can be retried
                query = text("""
                    UPDATE items 
                    SET is_processing = FALSE, processing_started = NULL
                    WHERE id = :item_id
                """)
            
            await session.execute(query, {"item_id": item_id})
            await session.commit()
        except Exception as e:
            print(f"Error releasing item {item_id}: {e}")
            await session.rollback()

async def poll_database():
    """Poll database for new items with distributed locking"""
    while True:
        try:
            # Try to claim the next item
            record = await claim_next_item()
            
            if not record:
                await asyncio.sleep(1)
                continue
                
            item_id, search = record
            print(f"Processing item {item_id} with search: {search}")
            
            try:
                processed_search = await oxy_search(search)
                if processed_search:
                    # Process links in a new session
                    async with get_session() as session:
                        async with session.begin():
                            for result in processed_search:
                                if not result.get('url'):
                                    continue
                                new_link = Link(
                                    item_id=item_id,
                                    url=result['url'],
                                    title=result['title'],
                                    photo_url=result['thumbnail'],
                                    price=result['price_str'],
                                    rating=result['rating'],
                                    reviews_count=result['reviews_count'],
                                    merchant_name=result['merchant_name']
                                )
                                session.add(new_link)
                            await session.commit()
                            print(f"Successfully added links for item {item_id}")
                    
                    # Mark as successfully processed
                    await release_item(item_id, success=True)
                else:
                    # No results but still mark as processed
                    await release_item(item_id, success=True)
                    print(f"No results found for item {item_id}")
                    
            except Exception as e:
                print(f"Error processing item {item_id}: {e}")
                # Release the item for retry
                await release_item(item_id, success=False)
                
        except Exception as e:
            print(f"Error during database polling: {e}")
            await asyncio.sleep(1)

async def oxy_search(query):
    """Sends a query to the Oxylabs API and returns structured search results."""
    print(f'Starting oxy_search for query: {query}')
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
            print(f"Found {len(records)} results for query: {query}")
            return records
    except Exception as e:
        print(f"Error during Oxylabs API call: {e}")
        return []

async def process_result(result):
    """Process a single result to extract necessary fields and get seller link."""
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

async def get_seller_link(product_id):
    payload = {
        'source': 'google_shopping_product',
        'domain': 'com',
        'pages': 1,
        'parse': True,
        'query': product_id
    }

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

@app.on_event("startup")
async def startup_event():
    # Add required columns if they don't exist
    async with engine.begin() as conn:
        await conn.execute(text("""
            DO $$ 
            BEGIN 
                IF NOT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name='items' AND column_name='is_processing'
                ) THEN 
                    ALTER TABLE items ADD COLUMN is_processing BOOLEAN DEFAULT FALSE;
                END IF;
                
                IF NOT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name='items' AND column_name='processing_started'
                ) THEN 
                    ALTER TABLE items ADD COLUMN processing_started FLOAT;
                END IF;
            END $$;
        """))
    
    asyncio.create_task(poll_database())

@app.get("/")
async def root():
    return {"message": "Polling the database for new records in the items table"}