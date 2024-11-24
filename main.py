import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from twikit import Client
import time
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RateLimiter:
    def __init__(self, requests_per_window: int, window_size: int = 60):
        self.requests_per_window = requests_per_window
        self.window_size = window_size  # in seconds
        self.requests = []
    
    async def wait_if_needed(self):
        """Wait if we've exceeded our rate limit"""
        now = time.time()
        
        # Remove old requests outside the window
        self.requests = [req_time for req_time in self.requests 
                        if now - req_time < self.window_size]
        
        if len(self.requests) >= self.requests_per_window:
            # Wait until the oldest request falls out of the window
            sleep_time = self.window_size - (now - self.requests[0])
            if sleep_time > 0:
                logger.info(f"Rate limit reached. Waiting for {sleep_time:.2f} seconds")
                await asyncio.sleep(sleep_time)
            self.requests = self.requests[1:]
        
        self.requests.append(now)

class TwitterScraper:
    def __init__(self, mongodb_uri: str, db_name: str):
        """Initialize the scraper with MongoDB connection details"""
        self.client = Client('en-US')
        self.mongo_client = MongoClient(mongodb_uri)
        self.db = self.mongo_client[db_name]
        self.rate_limiter = RateLimiter(requests_per_window=300)  # Twitter's standard rate limit
        
    def load_cookies(self, cookie_file: str):
        """Load Twitter cookies from a JSON file"""
        with open(cookie_file, "r") as f:
            cookies = json.load(f)
        self.client.set_cookies(cookies)

    def get_collection(self, collection_name: str) -> Collection:
        """Get or create a MongoDB collection"""
        return self.db[collection_name]

    async def fetch_tweet_details(self, tweet) -> Dict[Any, Any]:
        """Extract all relevant details from a tweet"""
        await self.rate_limiter.wait_if_needed()
        
        tweet_data = {
            'id': tweet.id,
            'created_at': tweet.created_at,
            'text': tweet.text,
            'full_text': tweet.full_text,
            'lang': tweet.lang,
            'favorite_count': tweet.favorite_count,
            'retweet_count': tweet.retweet_count,
            'reply_count': tweet.reply_count,
            'quote_count': tweet.quote_count,
            'view_count': tweet.view_count,
            'view_count_state': tweet.view_count_state,
            'is_quote_status': tweet.is_quote_status,
            'possibly_sensitive': tweet.possibly_sensitive,
            'in_reply_to': tweet.in_reply_to,
            'has_card': tweet.has_card,
            'hashtags': tweet.hashtags,
            'urls': tweet.urls,
            'scraped_at': datetime.utcnow()
        }

        # Add user information
        if tweet.user:
            tweet_data['user'] = {
                'id': tweet.user.id,
                'name': tweet.user.name,
                'screen_name': tweet.user.screen_name,
                'followers_count': tweet.user.followers_count,
                'following_count': tweet.user.following_count,
                'verified': tweet.user.verified
            }

        return tweet_data

    async def fetch_user_tweets(
        self, 
        screen_name: str, 
        tweet_types: List[str] = None,
        max_tweets_per_type: int = 100,
        collection_name: str = 'tweets'
    ):
        """
        Fetch tweets for a user across different tweet types
        """
        if tweet_types is None:
            tweet_types = ["Tweets", "Replies", "Media", "Likes"]

        collection = self.get_collection(collection_name)
        
        try:
            # Get user details
            await self.rate_limiter.wait_if_needed()
            user = await self.client.get_user_by_screen_name(screen_name)
            logger.info(f"Fetching tweets for user: {screen_name}")

            for tweet_type in tweet_types:
                tweets_fetched = 0
                cursor = None

                while tweets_fetched < max_tweets_per_type:
                    await self.rate_limiter.wait_if_needed()
                    
                    # Fetch batch of tweets
                    tweets = await self.client.get_user_tweets(
                        user.id,
                        tweet_type=tweet_type,
                        count=min(20, max_tweets_per_type - tweets_fetched),
                        cursor=cursor
                    )

                    if not tweets:
                        break

                    # Process each tweet
                    for tweet in tweets:
                        tweet_data = await self.fetch_tweet_details(tweet)
                        tweet_data['tweet_type'] = tweet_type
                        
                        # Use update_one with upsert to avoid duplicates
                        await self.rate_limiter.wait_if_needed()
                        collection.update_one(
                            {'id': tweet_data['id']},
                            {'$set': tweet_data},
                            upsert=True
                        )
                        tweets_fetched += 1

                    logger.info(f"Fetched {tweets_fetched} {tweet_type} for {screen_name}")
                    
                    # Prepare for next batch
                    if hasattr(tweets, 'next_cursor'):
                        cursor = tweets.next_cursor
                    else:
                        break

        except Exception as e:
            logger.error(f"Error fetching tweets for {screen_name}: {str(e)}")
            raise

    def close(self):
        """Close MongoDB connection"""
        self.mongo_client.close()

async def main():
    # Configuration
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "twitter_data"
    COOKIE_FILE = "cookies.json"
    SCREEN_NAMES = ["DCI_Kenya"]  # Add more handles as needed
    
    try:
        # Initialize scraper
        scraper = TwitterScraper(MONGODB_URI, DB_NAME)
        scraper.load_cookies(COOKIE_FILE)
        
        # Fetch tweets for each user
        for screen_name in SCREEN_NAMES:
            await scraper.fetch_user_tweets(
                screen_name=screen_name,
                tweet_types=["Tweets", "Replies", "Media", "Likes"],
                max_tweets_per_type=5,
                collection_name='tweets'
            )
            
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
    finally:
        scraper.close()

if __name__ == "__main__":
    asyncio.run(main())