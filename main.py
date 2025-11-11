import os
import requests
import json
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
import functions_framework 

# --- Load Credentials from Environment Variables ---
API_KEY = os.environ.get("NEWS_API_KEY")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
PUB_SUB_TOPIC = os.environ.get("PUB_SUB_TOPIC")

# --- Initialize clients (outside main function for reuse) ---
publisher_client = pubsub_v1.PublisherClient()
topic_path = publisher_client.topic_path(PROJECT_ID, PUB_SUB_TOPIC)

# --- THIS IS THE NEW FUNCTION SIGNATURE ---
@functions_framework.http
def poll_news_and_publish(request):
    """
    HTTP-triggered Cloud Run function.
    Polls NewsAPI for new articles and publishes them to Pub/Sub.
    """
    
    # --- Define Search Query ---
    query = "AI OR 'Large Language Models'" # Your keyword
    from_time = (datetime.utcnow() - timedelta(minutes=16)).isoformat()

    url = (
        "https://newsapi.org/v2/everything?"
        f"q={query}&"
        f"from={from_time}&"
        "sortBy=publishedAt&"
        f"apiKey={API_KEY}"
    )

    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an error on a bad response
        data = response.json()
        
        articles = data.get("articles", [])
        print(f"Found {len(articles)} new articles for query: '{query}'")

        if not articles:
            print("No new articles found. Exiting.")
            return "Success (No articles)", 200

        for article in articles:
            # ... (all the logic for creating and publishing the message is identical) ...
            message_data = {
                "source_name": article.get("source", {}).get("name"),
                "author": article.get("author"),
                "title": article.get("title"),
                "description": article.get("description"),
                "url": article.get("url"),
                "published_at": article.get("publishedAt")
            }
            
            message_bytes = json.dumps(message_data).encode("utf-8")
            future = publisher_client.publish(topic_path, data=message_bytes)
            print(f"Published message for article: {article.get('title')[:30]}...")

        print("Polling complete.")
        return "Success", 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return "Error", 500