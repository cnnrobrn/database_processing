from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import asyncio
from datetime import datetime
import os
from dotenv import load_dotenv
import requests
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
from flask import Flask

app = Flask(__name__)

db = SQLAlchemy(app)

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')
OXY_USERNAME= os.getenv("OXY_USERNAME")
OXY_PASSWORD= os.getenv("OXY_PASSWORD")

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False



class Link(db.Model):
    __tablename__ = 'links'
    id = db.Column(db.Integer, primary_key=True)
    item_id = db.Column(db.Integer, db.ForeignKey('items.id'), nullable=False)
    photo_url = db.Column(db.Text, nullable=True)  # Increased to accommodate long photo URLs
    url = db.Column(db.String(2000), nullable=False)  # Increased to accommodate longer URLs
    price = db.Column(db.String(200), nullable=True)  # Increase length if needed
    title = db.Column(db.String(2000), nullable=False)  # Increased to accommodate longer URLs
    rating = db.Column(db.Float, nullable=True)
    reviews_count = db.Column(db.Integer, nullable=True)
    merchant_name = db.Column(db.String(200), nullable=True)

# Database engine and session configuration
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# FastAPI app instance
app = FastAPI()

# Global variable to track the last time the database was checked
last_checked = datetime.utcnow()

async def poll_database():
    """
    Continuously polls the database for new records in the items table.
    When new records are found, it retrieves the Amazon_Search value.
    """
    global last_checked
    while True:
        async with async_session() as session:
            # Query to find new records in the items table
            query = """
            SELECT Amazon_Search 
            FROM items 
            WHERE created_at > :last_checked
            """
            result = await session.execute(query, {"last_checked": last_checked})
            new_records = result.fetchall()

            # If new records are found, process them
            if new_records:
                for record in new_records:
                    amazon_search = record["Amazon_Search"]
                    item_id = record["id"]
                    processed_search = oxy_search(amazon_search)
                    for result in processed_search:
                        if not result.get('url'):
                            continue
                        new_link = Link(
                            item_id=item_id.id,
                            url=result['url'],
                            title=result['title'],
                            photo_url=result['thumbnail'],
                            price=result['price'],
                            rating=result['rating'],
                            reviews_count=result['reviews_count'],
                            merchant_name=result['merchant_name']
                        )
                        db.session.add(new_link)
                    db.session.commit()
                    
                # Update the last_checked time to the current time
                last_checked = datetime.utcnow()

        # Sleep for a while before checking again (e.g., 5 seconds)
        await asyncio.sleep(1)

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
    # Structure payload.
    payload = {
    'source': 'google_shopping_search',
    'domain':'com',
    'pages': 1,
    'parse': True,
    'query': query
    }

    # Get response.
    print(f"Sending payload: {payload}")
    try:
        response = requests.request(
            'POST',
            'https://realtime.oxylabs.io/v1/queries',
            auth=(OXY_USERNAME, OXY_PASSWORD),
            json=payload,
            timeout=10
        )
        print(f"Response received: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        raise


    # Instead of response with job status and results url, this will return the
    # JSON response with results.
    data = response.json()
    organic_results = data["results"][0]["content"]["results"]["organic"][:30]


    structured_data = [
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

    return structured_data
