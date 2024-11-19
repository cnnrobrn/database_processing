from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import asyncio
import os
from dotenv import load_dotenv
import httpx
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
class Item(Base):
    __tablename__ = 'items'
    id = Column(Integer, primary_key=True)
    outfit_id = Column(Integer, ForeignKey('outfits.id'), nullable=False)
    description = Column(String, nullable=True)
    search = Column(String, nullable=True)
    processed_at = Column(Float, nullable=True)
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

async def get_last_checked_id():
    async with get_session() as session:
        try:
            query = text("SELECT COALESCE(MAX(item_id), 0) FROM links")
            result = await session.execute(query)
            return result.scalar()
        except Exception as e:
            print(f"Error fetching last_checked_id: {e}")
            return 0

async def poll_database():
    """Poll database for new items"""
    last_checked_id = await get_last_checked_id()
    print(f"Starting from ID: {last_checked_id}")

    while True:
        try:
            async with get_session() as session:
                # Get new items
                query = text("""
                    SELECT id, search 
                    FROM items 
                    WHERE id > :last_id
                    AND search IS NOT NULL
                    ORDER BY id
                    LIMIT 5
                """)
                
                result = await session.execute(query, {"last_id": last_checked_id})
                new_records = result.fetchall()

                if not new_records:
                    await asyncio.sleep(1)
                    continue

                # Process each record
                for record in new_records:
                    item_id, search = record
                    print(f"Processing item {item_id}")
                    
                    try:
                        processed_search = await oxy_search(search)
                        if processed_search:
                            # Add new links
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
                                print(f"Added links for item {item_id}")
                    except Exception as e:
                        print(f"Error processing item {item_id}: {e}")
                        await session.rollback()
                    
                    # Update the last checked ID regardless of success
                    last_checked_id = item_id
                    
        except Exception as e:
            print(f"Error during database polling: {e}")
            await asyncio.sleep(1)

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
            print(f"Found {len(records)} results")
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
    
    asyncio.create_task(poll_database())

@app.get("/")
async def root():
    return {"message": "Polling the database for new records in the items table"}