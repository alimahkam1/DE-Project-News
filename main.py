import os
import requests
import json
from google.cloud import pubsub_v1
import functions_framework

# --- Initialize clients in global scope (this is safe) ---
publisher_client = pubsub_v1.PublisherClient()

# --- The API URL (no key needed) ---
ISS_API_URL = "http://api.open-notify.org/iss-now.json"

@functions_framework.http
def poll_iss_and_publish(request):
    """
    HTTP-triggered Cloud Run function.
    Polls the ISS API for its current location and publishes to Pub/Sub.
    """

    # --- Get env variables INSIDE the function ---
    try:
        PROJECT_ID = os.environ["GCP_PROJECT_ID"]
        PUB_SUB_TOPIC = os.environ["PUB_SUB_TOPIC"]

        # Build the topic path *inside* the function
        topic_path = publisher_client.topic_path(PROJECT_ID, PUB_SUB_TOPIC)

    except KeyError as e:
        # This gives us a clear error if a variable is missing!
        print(f"CRITICAL ERROR: Environment variable {e} is not set.")
        return f"Error: Missing configuration {e}", 500

    try:
        response = requests.get(ISS_API_URL)
        response.raise_for_status() # Raise an error on a bad response
        data = response.json()

        if data.get("message") == "success":
            message_data = {
                "latitude": data.get("iss_position", {}).get("latitude"),
                "longitude": data.get("iss_position", {}).get("longitude"),
                "timestamp": data.get("timestamp")
            }

            message_bytes = json.dumps(message_data).encode("utf-8")
            future = publisher_client.publish(topic_path, data=message_bytes)
            print(f"Published ISS location: {message_data}")

            return "Success", 200
        else:
            print("API call did not return 'success'")
            return "Error (API Failed)", 500

    except Exception as e:
        print(f"An error occurred: {e}")
        return "Error", 500
