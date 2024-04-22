import urllib.request
import json
from google.cloud import pubsub_v1

# GCP Configuration
project_id = 'focus-surfer-420318'
topic_id = 'busBreadCrumbData'

# Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

vehicle_ids = [
    3029, 3235, 4027, 3608, 3059, 3213, 3507, 2902, 3721, 3024,
    3555, 3702, 3146, 2904, 3018, 3704, 3542, 2935, 3244, 3234,
    4205, 4526, 3119, 4520, 2928, 3019, 3650, 3538, 3505, 3558,
    3411, 3115, 3638, 4502, 3546, 3021, 3258, 3169, 3046, 3254,
    3419, 4011, 2924, 3030, 3408, 3201, 4211, 3719, 3756, 3267,
    3266, 4505, 3557, 3525, 3017, 4210, 3325, 3054, 3728, 3802,
    3005, 3035, 3937, 3420, 3530, 4007, 3630, 3705, 3622, 3631,
    3503, 3212, 3724, 4070, 3910, 3964, 3012, 3634, 3605, 4236,
    4518, 3559, 4237, 3710, 3237, 3906, 3644, 3805, 3513, 4048,
    4207, 3627, 3524, 3727, 3733, 3518, 3045, 3023, 3904, 3734
]

def fetch_and_publish_data(vehicle_id):
    """Fetch JSON data for a vehicle and publish each record to GCP Pub/Sub."""
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
            # Assuming the JSON structure contains a list of records
            for record in data:
                publish_to_pubsub(json.dumps(record),vehicle_id)
            print(f"Processed vehicle ID {vehicle_id}")
    except urllib.error.URLError as e:
        print(f"Failed to fetch data for vehicle ID {vehicle_id}: {e}")

def publish_to_pubsub(message,vehicle_id):
    """Publish a message to the configured GCP Pub/Sub topic."""
    try:
        message_bytes = message.encode('utf-8')
        future = publisher.publish(topic_path, message_bytes)
        #print(f"Message published. ID: {future.result()}")
    except Exception as e:
        print(f"An error occurred while publishing: {e} {vehicle_id}")

def main():
    for vehicle_id in vehicle_ids:
        fetch_and_publish_data(vehicle_id)

if __name__ == "__main__":
    main()
