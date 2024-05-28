import json
import psycopg2
from psycopg2 import pool, extras
from google.cloud import pubsub_v1
from datetime import datetime
import threading
from threading import Lock

# GCP Configuration
project_id = 'focus-surfer-420318'
trip_subscription_id = 'busstopdata-sub'  # Subscription for the second queue

# Database configuration
DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgresql1234"

# Initialize the connection pool
db_pool = pool.SimpleConnectionPool(1, 10, host="localhost", database=DBname, user=DBuser, password=DBpwd)

def get_db_connection():
    # Get a connection from the connection pool
    conn = db_pool.getconn()
    if conn:
        conn.autocommit = True
    try:
        # Set session-specific parameters
        set_session_parameters(conn)
    except Exception as e:
        print("Failed to set session parameters:", e)
        # Optionally return the connection back to the pool if unable to set parameters
        db_pool.putconn(conn, close=True)
        conn = None  # Ensure that faulty connection isn't used
    return conn

def release_db_connection(conn):
    # Return the connection to the pool
    db_pool.putconn(conn)

def set_session_parameters(conn):
    # Open a cursor to perform database operations
    with conn.cursor() as cur:
        # Set the work_mem for this session
        cur.execute("SET work_mem TO '1GB';")
        
        # Set the maintenance_work_mem for this session
        cur.execute("SET maintenance_work_mem TO '1GB';")
        
        # You can set other parameters as needed
        cur.execute("SET temp_buffers TO '1GB';")

def map_service_key(service_key):
    """Map service_key values to the corresponding enum."""
    if service_key == 'S':
        return 'Saturday'
    elif service_key == 'U':
        return 'Sunday'
    else:
        return 'Weekday'

def map_direction(direction):
    """Map direction values to the corresponding enum."""
    if direction == '0':
        return 'Out'
    elif direction == '1':
        return 'Back'
    else:
        return None

def validate_data(trip_id, route_id, service_key, direction):
    """Validate the extracted data."""
    assert isinstance(trip_id, str) and trip_id.isdigit() and len(trip_id) == 9, "Invalid trip_id"
    assert trip_id.isdigit() and int(trip_id) > 0, "trip_id should be a positive number and 9 digits long"
    
    assert route_id.isdigit() and int(route_id) > 0, "route_number should be a positive integer"
    
    assert direction in ['0', '1'], "direction should be either 0 or 1"
    
    assert service_key.isalpha() and len(service_key) == 1, "service_key should be a single alphabet"

def trip_callback(message):
    """Callback function to handle messages from the second queue."""
    global db_pool
    try:
        data = json.loads(message.data.decode('utf-8'))
        
        # Extract data for the Trip table
        trip_id = data.get('trip_id')
        route_id = data.get('route_number')
        service_key = data.get('service_key')
        direction = data.get('direction')
        
        # Validate the extracted data
        try:
            validate_data(trip_id, route_id, service_key, direction)
        except AssertionError as e:
            print(f"Validation failed: {e}")
            message.ack()  # Acknowledge the message even if validation fails
            return

        mapped_service_key = map_service_key(service_key)
        mapped_direction = map_direction(direction)
        
        if trip_id and route_id and mapped_service_key and mapped_direction:
            conn = get_db_connection()
            if conn is None:
                print("Failed to get a database connection.")
                return

            try:
                with conn.cursor() as cur:
                    # Attempt to update the Trip table
                    update_query = """
                        UPDATE Trip
                        SET route_id = %s, service_key = %s, direction = %s
                        WHERE trip_id = %s;
                    """
                    cur.execute(update_query, (route_id, mapped_service_key, mapped_direction, trip_id))
                    if cur.rowcount == 0:
                        # If no rows were affected, insert into the errorTripUpdates table
                        error_query = """
                            INSERT INTO errorTripUpdates (trip_id, route_id, service_key, direction)
                            VALUES (%s, %s, %s, %s);
                        """
                        cur.execute(error_query, (trip_id, route_id, mapped_service_key, mapped_direction))
                        conn.commit()
                        print(f"Inserted trip_id {trip_id} into errorTripUpdates table.")
                    else:
                        conn.commit()
                        print(f"Updated trip_id {trip_id} in Trip table.")
            except Exception as e:
                print(f"Failed to update trip data: {e}")
            finally:
                release_db_connection(conn)
        else:
            print(f"Invalid data received: {data}")

        message.ack()  # Acknowledge the message after processing

    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, trip_subscription_id)

    while True:
        try:
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=trip_callback)
            print(f"Listening for messages on {subscription_path}...")
            streaming_pull_future.result()
        except KeyboardInterrupt:
            print("Process interrupted by user.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            streaming_pull_future.cancel()
            streaming_pull_future.result()
            print("Attempting to reconnect after timeout or error...")

    subscriber.close()
    print("Subscriber closed.")

if __name__ == "__main__":
    main()
