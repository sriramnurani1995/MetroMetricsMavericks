import json
import pandas as pd
import psycopg2
from psycopg2 import pool, extras
from google.cloud import pubsub_v1, storage
from datetime import datetime, timedelta
import threading
from threading import Lock
import zlib
import rsa
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
import os
import time

# GCP Configuration
project_id = 'focus-surfer-420318'
subscription_id = 'busBreadCrumbData-sub'
bucket_name = 'projectarchival'  

# Database configuration
DBname = "postgres"
DBuser = "postgres"
DBpwd = "postgresql1234"

# Initialize the connection pool
db_pool = pool.SimpleConnectionPool(1, 10, host="localhost", database=DBname, user=DBuser, password=DBpwd)

# Timeout configuration
timeout_minutes = 3
last_message_time = datetime.now()
# Global lock for synchronizing access to batch_data
batch_data_lock = Lock()
batch_data = []
batch_size = 100  # Define an appropriate size for batching

# Initialize storage client
storage_client = storage.Client()
today = datetime.now().strftime("%Y-%m-%d")
file_name = f"archive_{today}.json.gz.enc"  # Updated file extension to indicate encryption
blob = storage_client.bucket(bucket_name).blob(file_name)

# Load RSA keys from files
with open('public_key.pem', 'rb') as f:
    public_key = rsa.PublicKey.load_pkcs1(f.read())

with open('private_key.pem', 'rb') as f:
    private_key = rsa.PrivateKey.load_pkcs1(f.read())

# Generate a random AES key
aes_key = os.urandom(32)

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

class PersistentTimer:
    def __init__(self, action, check_interval=60, timeout_minutes=3):
        self.timeout_minutes = timeout_minutes
        self.action = action
        self.check_interval = check_interval
        self.timer = threading.Timer(self.check_interval, self.run)
        self._lock = threading.Lock()

    def run(self):
        with self._lock:
            global last_message_time
            
            if datetime.now() - last_message_time >= timedelta(minutes=self.timeout_minutes):
                self.action()
            self.timer = threading.Timer(self.check_interval, self.run)
            self.timer.start()

    def start(self):
        with self._lock:
            self.timer.start()

    def stop(self):
        with self._lock:
            if self.timer is not None:
                self.timer.cancel()
                self.timer = None

def compress_data(data):
    """Compress data using zlib."""
    return zlib.compress(data.encode('utf-8'))

def encrypt_data_with_aes(data, key):
    """Encrypt data using AES."""
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(data) + padder.finalize()
    
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
    return iv + encrypted_data

def decrypt_data_with_aes(encrypted_data, key):
    """Decrypt data using AES."""
    iv = encrypted_data[:16]
    encrypted_data = encrypted_data[16:]

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    padded_data = decryptor.update(encrypted_data) + decryptor.finalize()

    unpadder = padding.PKCS7(128).unpadder()
    data = unpadder.update(padded_data) + unpadder.finalize()
    return data

def upload_data_to_gcs(data):
    """Compress, encrypt, and upload data to GCS."""
    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        try:
            existing_data = b""
            if blob.exists():
                existing_encrypted_data = blob.download_as_bytes()
                encrypted_key = existing_encrypted_data[:256]
                encrypted_data = existing_encrypted_data[256:]
                decrypted_key = rsa.decrypt(encrypted_key, private_key)
                existing_compressed_data = decrypt_data_with_aes(encrypted_data, decrypted_key)
                existing_data = zlib.decompress(existing_compressed_data)
            existing_json = json.loads(existing_data.decode('utf-8')) if existing_data else []
            existing_json.extend(data)
            updated_data_string = json.dumps(existing_json)
            compressed_data = compress_data(updated_data_string)
            encrypted_data = encrypt_data_with_aes(compressed_data, aes_key)
            encrypted_key = rsa.encrypt(aes_key, public_key)

            final_data = encrypted_key + encrypted_data
            blob.upload_from_string(final_data, content_type='application/octet-stream')
            print(f"Uploaded {len(data)} messages to {bucket_name}/{file_name}")
            break  # Exit loop if upload is successful
        except Exception as e:
            retry_count += 1
            wait_time = min(2 ** retry_count, 30)  # Exponential backoff with cap
            print(f"An error occurred during upload (attempt {retry_count}): {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    else:
        print(f"Failed to upload after {max_retries} retries.")

def cancel_subscription():
    """Action to perform when the timeout is reached."""
    global batch_data, last_message_time
    last_message_time = datetime.now()
    print("More than 3 minutes elapsed since the last message. Proceeding to Process the Messages and Restart Subscription")
    streaming_pull_future.cancel()
    
    with batch_data_lock:  # Locking around access to batch_data
        if batch_data:
            df = pd.DataFrame(batch_data)
            df['TIMESTAMP'] = df.apply(lambda row: decode_timestamp(row['OPD_DATE'], row['ACT_TIME']), axis=1)
            df = df.drop(columns=['OPD_DATE', 'ACT_TIME', 'GPS_SATELLITES', 'GPS_HDOP'])
            df['GPS_LONGITUDE'] = df['GPS_LONGITUDE'].astype(float).fillna(0)
            df['GPS_LATITUDE'] = df['GPS_LATITUDE'].astype(float).fillna(0)
            bulk_insert(df)
            upload_data_to_gcs(batch_data)  # Upload to GCS
            batch_data = []  # Reset the batch data after processing
    try:
        updateforzerolatlong()
        query="select * from TempBreadCrumbs order by event_no_trip,timestamp asc"
        df2=fetch_data_to_dataframe(query)
        if not df2.empty:
            checkforAssertions(df2)
            # Speed Transformation
        
            df2["dmeters"] = df2.groupby(["event_no_trip"])["meters"].diff()
            df2["dtimestamp"] = df2.groupby(["event_no_trip"])["timestamp"].diff()
            df2["speed"] = df2.apply(
                lambda row: round(row["dmeters"] / row["dtimestamp"].total_seconds(), 2)
                if pd.notnull(row["dtimestamp"]) and row["dtimestamp"].total_seconds() > 0
                else 0,
                axis=1
            )

            def replace_first_speed(group):
                if group.iloc[0]['speed'] == 0 and len(group) > 1:
                    group.iloc[0, group.columns.get_loc('speed')] = group.iloc[1]['speed']
                return group

            df2 = df2.groupby('event_no_trip').apply(replace_first_speed)
    
            checkforTransformedAssertions(df2)
            createUniqueTripRecords()
            df2=df2.drop(['event_no_stop','vehicle_id','meters'], axis=1)
            df2.columns = df2.columns.str.replace('event_no_trip', 'trip_id')
            df2.columns = df2.columns.str.replace('gps_latitude', 'latitude')
            df2.columns = df2.columns.str.replace('gps_longitude', 'longitude')
            df2.columns = df2.columns.str.replace('timestamp', 'tstamp')
            df2.columns = df2.columns.str.replace('SPEED', 'speed')
            bulk_insert_breadCrumb(df2)
            updatefortruncateTempTable()
    except Exception as e:
        print(f"Cancel Subscription Method Failed: {e}")

def checkforAssertions(df2):
    try:
        print("Check For Assertions")
        assert (df2["vehicle_id"] > 0).all(), "Vehicle ID should be positive"
        assert df2["event_no_trip"].between(100000000, 999999999).all(), "EVENT_NO_TRIP should be a positive nine-digit number"
        assert df2["event_no_stop"].between(100000000, 999999999).all(), "EVENT_NO_STOP should be a positive nine-digit number"
        assert df2['gps_latitude'].between(42, 46.5).all(), "Latitude must be between a value of Oregon's Range"
        assert df2['gps_longitude'].between(-124.5, -116.5).all(), "Longitude must be between a value of Oregon's Range"
        assert pd.api.types.is_datetime64_any_dtype(df2['timestamp']), "All entries in 'TIMESTAMP' column must be datetime objects."
        formatted_dates = df2['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
        assert formatted_dates is not None, "Timestamp is in Expected Format"
        print("Assertion 8: Checking that every record has a stop number.")
        # Assert that there are no missing values in the 'STOP_NUMBER' column
        assert df2['event_no_stop'].notna().all(), "Some records are missing a stop number!"
        print("All records have a stop number.")
        print("\nAssertion 9  :Non-Negative Meter Reading")
        if (df2['meters'] < 0).any():
            print("Negative values found in METERS!")
        else:
            print("All meter readings are non-negative.")
    except Exception as e:
        print(f"An Assertion Failed: {e}")

def checkforTransformedAssertions(df2):
    try:
        assert (df2["speed"] >= 0).all(), "All speed values should be non-negative"
        print("\nAssertion 3: Maximum Speed Reasonability Check")
        assert (df2['speed'] <= 50).all(), "All speed values are within the reasonable range."
    except Exception as e:
        print(f"An Transformed Assertion Failed: {e}")

def callback(message):
    global last_message_time, batch_data
    try:
        data = json.loads(message.data.decode('utf-8'))
        
        with batch_data_lock:  # Locking around access to batch_data
            batch_data.append(data)
            # Check if the batch size has been reached or exceeded
            if len(batch_data) >= batch_size:
                df = pd.DataFrame(batch_data)
                df['TIMESTAMP'] = df.apply(lambda row: decode_timestamp(row['OPD_DATE'], row['ACT_TIME']), axis=1)
                df = df.drop(columns=['OPD_DATE', 'ACT_TIME', 'GPS_SATELLITES', 'GPS_HDOP'])
                df['GPS_LONGITUDE'] = df['GPS_LONGITUDE'].astype(float).fillna(0)
                df['GPS_LATITUDE'] = df['GPS_LATITUDE'].astype(float).fillna(0)
                bulk_insert(df)
                upload_data_to_gcs(batch_data)  # Upload to GCS
                batch_data = []  # Reset the batch data after processing

        message.ack()  # Acknowledge the message outside the lock
        last_message_time = datetime.now()

    except Exception as e:
        print(f"An error occurred: {e}")

def decode_timestamp(opd_date, act_time):
    return datetime.strptime(opd_date, "%d%b%Y:%H:%M:%S") + timedelta(seconds=act_time)

def bulk_insert(df):
    conn = get_db_connection()
    if conn is None:
        print("Failed to get a database connection.")
        return

    try:
        with conn.cursor() as cur:
            # Ensure that the DataFrame columns match the database schema exactly
            expected_cols = ['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'VEHICLE_ID', 'METERS', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'TIMESTAMP']
            if not all(col in df.columns for col in expected_cols):
                print("DataFrame columns do not match expected:", df.columns)
                return
            
            tuples = [tuple(x) for x in df[expected_cols].to_numpy()]
            cols = ','.join(expected_cols)
            query = "INSERT INTO TempBreadCrumbs (" + cols + ") VALUES %s"
            psycopg2.extras.execute_values(cur, query, tuples)
            conn.commit()
    except Exception as e:
        print("Failed to insert batch:", e)
    finally:
        release_db_connection(conn)

def bulk_insert_breadCrumb(df):
    conn = get_db_connection()
    if conn is None:
        print("Failed to get a database connection.")
        return

    try:
        with conn.cursor() as cur:
            # Ensure that the DataFrame columns match the database schema exactly
            expected_cols = ['trip_id', 'longitude', 'latitude', 'tstamp', 'speed']
            if not all(col in df.columns for col in expected_cols):
                print("DataFrame columns do not match expected:", df.columns)
                return
            
            tuples = [tuple(x) for x in df[expected_cols].to_numpy()]
            cols = ','.join(expected_cols)
            query = "INSERT INTO BreadCrumb (" + cols + ") VALUES %s"
            psycopg2.extras.execute_values(cur, query, tuples)
            conn.commit()
    except Exception as e:
        print("Failed to insert batch:", e)
    finally:
        release_db_connection(conn)

def updateforzerolatlong():
    conn = get_db_connection()
    if conn is None:
        print("Failed to get a database connection.")
        return

    try:
        with conn.cursor() as cur:
             cur.execute("""UPDATE TempBreadCrumbs AS a SET gps_latitude = 46.4 WHERE a.gps_latitude = 0
                    """)
        
             cur.execute(""" UPDATE TempBreadCrumbs AS a SET gps_longitude = -124 WHERE a.gps_longitude = 0
                    """)
                
        conn.commit()
    except Exception as e:
        print("Failed to insert batch:", e)
    finally:
        release_db_connection(conn)

def updatefortruncateTempTable():
    conn = get_db_connection()
    if conn is None:
        print("Failed to get a database connection.")
        return

    try:
        with conn.cursor() as cur:
             cur.execute("""truncate table TempBreadCrumbs""")
        
                
        conn.commit()
    except Exception as e:
        print("Failed to insert batch:", e)
    finally:
        release_db_connection(conn)

def createUniqueTripRecords():
    conn = get_db_connection()
    if conn is None:
        print("Failed to get a database connection.")
        return

    try:
        with conn.cursor() as cur:
             cur.execute("""insert into Trip(trip_id,vehicle_id)  select event_no_trip,vehicle_id from TempBreadCrumbs group by event_no_trip,vehicle_id
                    """)
                
        conn.commit()
    except Exception as e:
        print("Failed to insert Trip:", e)
    finally:
        release_db_connection(conn)

def fetch_data_to_dataframe(sql_query):
    """
    Fetch data from PostgreSQL database and return it as a pandas DataFrame.
    
    :param sql_query: SQL query string
    :return: DataFrame containing the queried data
    """
    conn = get_db_connection()
    if conn is None:
        print("Failed to get a database connection for fetching data.")
        return None

    try:
        df = pd.read_sql_query(sql_query, conn)
        print("Data fetched successfully.")
        return df
    except Exception as e:
        print(f"Failed to fetch data: {e}")
        return None
    finally:
        if conn:
            release_db_connection(conn)

def main():
    global streaming_pull_future, subscriber
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    while True:
        timer = PersistentTimer(cancel_subscription)
        timer.start()
        try:
            streaming_pull_future = subscriber.subscribe(subscription_path, callback)
            print(f"Listening for messages on {subscription_path}...")
            streaming_pull_future.result()
        except KeyboardInterrupt:
            print("Process interrupted by user.")
            break
        except TimeoutError:
            print("Timeout reached with no messages.")
        finally:
            streaming_pull_future.cancel()
            timer.stop()
            print("Subscriber and timer stopped.")
        print("Attempting to reconnect after timeout or error...")

    subscriber.close()
    print("Subscriber closed.")

if __name__ == "__main__":
    main()
