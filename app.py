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
engine = create_async_engine(
    DATABASE_URL,
    echo=True,
    pool_size=5,
    max_overflow=0
)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Database Models
class Item(Base):
    __tablename__ = 'items'
    id = Column(Integer, primary_key=True)
    outfit_id = Column(Integer, ForeignKey('outfits.id'), nullable=False)
    description = Column(String, nullable=True)
    search = Column(String, nullable=True)
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

async def get_max_processed_id():
    """Get the highest processed item_id"""
    async with get_session() as session:
        query = text("SELECT COALESCE(MAX(item_id), 0) FROM links")
        result = await session.execute(query)
        return result.scalar()

async def get_and_process_next_item(last_processed_id: int):
    """Get and process the next available item after the last processed ID"""
    async with get_session() as session:
        try:
            # Get next unprocessed item after last_processed_id
            query = text("""
                WITH next_item AS (
                    SELECT id, search
                    FROM items i
                    WHERE id > :last_id
                    AND NOT EXISTS (
                        SELECT 1 FROM links l WHERE l.item_id = i.id
                    )
                    AND search IS NOT NULL
                    ORDER BY id
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                SELECT id, search FROM next_item;
            """)
            
            result = await session.execute(query, {"last_id": last_processed_id})
            record = result.first()
            
            if not record:
                return last_processed_id, False
                
            item_id, search = record
            print(f"Processing item {item_id} (after {last_processed_id})")
            
            # Process the item
            processed_search = await oxy_search(search)
            if processed_search:
                link_objects = []
                for result in processed_search:
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
                    await session.commit()
                    print(f"Successfully processed item {item_id}")
                    return item_id, True
            
            # Even if no results, commit to release the lock
            await session.commit()
            return item_id, True
            
        except Exception as e:
            print(f"Error processing item: {e}")
            await session.rollback()
            return last_processed_id, False

async def poll_database():
    """Poll database with progressive processing"""
    # Start from the highest processed ID
    last_processed_id = await get_max_processed_id()
    print(f"Starting from last processed ID: {last_processed_id}")

    while True:
        try:
            last_processed_id, success = await get_and_process_next_item(last_processed_id)
            if not success:
                # If no items to process, wait a bit
                await asyncio.sleep(1)
        except Exception as e:
            print(f"Error in poll_database: {e}")
            await asyncio.sleep(1)

async def oxy_search(query):
    """Sends a query to the Oxylabs API and returns structured search results."""
    print(f'oxy_search for: {query}')
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
            
            # Add error checking for the response
            if 'results' not in data:
                print(f"Unexpected API response: {data}")
                return []
                
            organic_results = data["results"][0]["content"]["results"]["organic"][:30]
            
            # Process results concurrently but with a small delay to avoid rate limits
            tasks = []
            for result in organic_results:
                task = asyncio.create_task(process_result(result))
                tasks.append(task)
                await asyncio.sleep(0.1)  # Small delay between API calls
                
            records = await asyncio.gather(*tasks)
            print(f"Found {len(records)} results")
            return records
    except Exception as e:
        print(f"Error during Oxylabs API call: {e}")
        return []

async def process_result(result):
    """Process a single result to extract necessary fields and get seller link."""
    product_id = result.get("product_id")
    
    return {
        "pos": result.get("pos"),
        "price_str": result.get("price_str"),
        "title": result.get("title"),
        "thumbnail": result.get("thumbnail"),
        "url": f"https://www.google.com/shopping/product/{product_id}",
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
            if 'results' not in data:
                print(f"Unexpected API response for seller link: {data}")
                return None
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
    asyncio.create_task(poll_database())

@app.get("/")
async def root():
    return {"message": "Processing items"}
