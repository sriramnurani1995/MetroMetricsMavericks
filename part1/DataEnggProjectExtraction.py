import os
import urllib.request
import json
import time
import logging
from datetime import datetime
from shutil import disk_usage
import smtplib
from email.message import EmailMessage

# Configure logging
logging.basicConfig(filename='vehicle_data_download_errors.log', level=logging.ERROR, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Email configuration
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 465
SENDER_EMAIL = 'metrometricsmavericks@gmail.com'  
SENDER_PASSWORD = 'pufiwtjxnoejtrty'
RECEIVER_EMAIL = 'srirams@pdx.edu'  # Replace with your receiver email

# Function to check if there is enough free disk space to save the file
def check_disk_space(folder_path, required_bytes):
    total, used, free = disk_usage(folder_path)
    return free >= required_bytes

# Function to download data with retries and exponential backoff
def download_data(vehicle_id, folder_name, max_retries=3):
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    retry_delay = 1
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                content = response.read()
                # Validate JSON format
                json.loads(content)
                file_path = os.path.join(folder_name, f"{vehicle_id}.json")
                # Check for available disk space before saving
                if check_disk_space(folder_name, len(content)):
                    with open(file_path, 'wb') as file:
                        file.write(content)
                        print(f"Successfully downloaded data for vehicle ID: {vehicle_id}")
                    return True
                else:
                    logging.error(f"Not enough disk space for vehicle ID {vehicle_id}")
                    return False
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} for vehicle ID {vehicle_id} failed with error: {e}")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
    return False



def send_email(subject, body, recipient):

    message = EmailMessage()
    message.set_content(body)
    message['Subject'] = subject
    message['From'] = SENDER_EMAIL
    message['To'] = recipient

    # Use smtplib.SMTP_SSL for SSL
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(message)


# Create a folder with today's date
today = datetime.now().strftime('%Y-%m-%d')
folder_name = f"vehicle_data_{today}"
os.makedirs(folder_name, exist_ok=True)

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

# Main script execution
failed_vehicle_ids = []
for vehicle_id in vehicle_ids:
    success = download_data(vehicle_id, folder_name)
    if not success:
        failed_vehicle_ids.append(vehicle_id)

# If there were failed downloads, send an email notification
if failed_vehicle_ids:
    subject = "Vehicle Data Download Failures"
    body = f"The following vehicle IDs failed to download data: {', '.join(map(str, failed_vehicle_ids))}"
    send_email(subject, body, RECEIVER_EMAIL)
    print(f"Notification sent for failed vehicle IDs: {failed_vehicle_ids}")

print("Download process completed.")
