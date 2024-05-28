import urllib.request
import json
from google.cloud import pubsub_v1
import requests
from bs4 import BeautifulSoup
import time

# GCP Configuration
project_id = 'focus-surfer-420318'
topic_id = 'busstopdata'

# Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

total_messages_sent = 0
total_messages_published = 0

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

def publish_callback(future, vehicle_id, counter):
    """Handles the result of the asynchronous publish call."""
    global total_messages_published
    try:
        future.result()
        total_messages_published += 1  
    except Exception as e:
        print(f"Failed to publish message for vehicle ID {vehicle_id}: {e}")
    finally:
        # Decrease the counter for each callback completion
        counter['count'] -= 1

def fetch_and_publish_data(vehicle_id):
    """Fetch HTML data for a vehicle and publish each record to GCP Pub/Sub."""
    global total_messages_sent
    url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_id}"
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all trip_id elements
        trip_elements = soup.find_all('h2', string=lambda text: 'Stop events for PDX_TRIP' in text)
        counter = {'count': 0}

        for trip_element in trip_elements:
            trip_id = trip_element.text.split(' ')[-1]

            # Find the corresponding table and extract the first row
            table = trip_element.find_next('table')
            first_row = table.find_all('tr')[1]  # Skip header row
            columns = first_row.find_all('td')
            
            route_number = columns[3].text.strip()
            direction = columns[4].text.strip()
            service_key = columns[5].text.strip()

            # Prepare data to publish
            record = {
                'trip_id': trip_id,
                'route_number': route_number,
                'direction': direction,
                'service_key': service_key
            }

            # Convert record to JSON string
            record_str = json.dumps(record)
            print(record_str)

            # Publish the message
            future = publisher.publish(topic_path, data=record_str.encode('utf-8'))
            future.add_done_callback(lambda f: publish_callback(f, vehicle_id, counter))
            total_messages_sent += 1
            counter['count'] += 1
        
        # Wait for all messages for the current vehicle to be published
        while counter['count'] > 0:
            time.sleep(1)
        
        print(f"Finished processing for vehicle ID {vehicle_id}")
    
    except Exception as e:
        print(f"Failed to fetch or publish data for vehicle ID {vehicle_id}: {e}")

# Process each vehicle ID sequentially
for vehicle_id in vehicle_ids:
    fetch_and_publish_data(vehicle_id)

print(f"Total messages sent: {total_messages_sent}")
print(f"Total messages published: {total_messages_published}")
