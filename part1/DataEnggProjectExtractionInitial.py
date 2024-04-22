import os
import requests
from datetime import datetime

# Create a folder with today's date
today = datetime.now().strftime('%Y-%m-%d')
folder_name = f"vehicle_data_{today}"
# Ensure the directory exists
os.makedirs(folder_name, exist_ok=True)

# List of vehicle IDs provided by you
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

# Download breadcrumb data for each vehicle
for vehicle_id in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()  # Raise an error for bad status codes

        # Save the response content to a file
        file_path = os.path.join(folder_name, f"{vehicle_id}.json")
        with open(file_path, 'wb') as file:
            file.write(response.content)

        print(f"Successfully downloaded data for vehicle ID: {vehicle_id}")

    except requests.RequestException as e:
        print(f"An error occurred for vehicle ID {vehicle_id}: {e}")

print("Download process completed.")
