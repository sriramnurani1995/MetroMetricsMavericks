import os
import json
from google.cloud import pubsub_v1
import threading

# GCP Configuration
project_id = 'focus-surfer-420318'
topic_id = 'busBreadCrumbData'

# Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

total_messages_sent = 0
total_messages_published = 0

# Folder containing JSON files
folder_path = '/home/srirams/vehicle_data_2024-05-01'

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

def process_json_file(file_path, vehicle_id):
    """Read JSON data from a file and publish each record to GCP Pub/Sub, wait for all messages to be published."""
    global total_messages_sent
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            print(f"Starting processing for vehicle ID {vehicle_id}")
            counter = {'count': len(data)}
            for record in data:
                publish_to_pubsub(json.dumps(record), vehicle_id, counter)
                total_messages_sent += 1
            # Wait until all messages are published
            while counter['count'] > 0:
                pass  # This could be replaced with a more sophisticated waiting mechanism
            print(f"Finished processing vehicle ID {vehicle_id}")
    except Exception as e:
        print(f"Failed to process file {file_path} for vehicle ID {vehicle_id}: {e}")

def publish_to_pubsub(message, vehicle_id, counter):
    """Publish a message to the configured GCP Pub/Sub topic."""
    message_bytes = message.encode('utf-8')
    future = publisher.publish(topic_path, message_bytes)
    future.add_done_callback(lambda f: publish_callback(f, vehicle_id, counter))

def main():
    # Read each file in the specified folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)
            vehicle_id = filename[:-5]  # Assuming filename is the vehicle ID plus '.json'
            process_json_file(file_path, vehicle_id)
    print(f"Total messages sent: {total_messages_sent}")
    print(f"Total messages published: {total_messages_published}")

if __name__ == "__main__":
    main()
