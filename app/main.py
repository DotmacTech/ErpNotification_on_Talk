import os
import json
import logging
import requests
from flask import Flask, request, g
from dotenv import load_dotenv
import time
import sqlite3

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
print("Environment variables loaded.")

# Configure logging
logging.basicConfig(filename='logs/app.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)
print("Logging configured.")

# Initialize Flask app
app = Flask(__name__)
print("Flask app initialized.")

ERPNEXT_API_URL = os.getenv('ERPNEXT_API_URL')
ERPNEXT_API_KEY = os.getenv('ERPNEXT_API_KEY')
ERPNEXT_API_SECRET = os.getenv('ERPNEXT_API_SECRET')
NEXTCLOUD_API_URL = os.getenv('NEXTCLOUD_API_URL')
NEXTCLOUD_USERNAME = os.getenv('NEXTCLOUD_USERNAME')
NEXTCLOUD_APP_PASSWORD = os.getenv('NEXTCLOUD_APP_PASSWORD')
TALK_BOT_USERNAME = os.getenv('TALK_BOT_USERNAME', 'ERPNext')
print("Environment variables read into application.")

# Database setup
DATABASE = 'erpnext_talk.db'


def get_db():
    """Gets the database connection."""
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row  # Return rows as dictionaries
    return g.db


@app.teardown_appcontext
def close_db(e=None):
    """Closes the database connection."""
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    """Initializes the database schema."""
    with app.app_context():
        db = get_db()
        with app.open_resource('schema.sql', mode='r') as f:
            db.executescript(f.read())
        db.commit()
print("DB initialized")
# Create the database tables if they don't exist
with app.app_context():
    init_db()


def fetch_user_details(email):
    """Fetches user details from ERPNext based on the email address."""
    print(f"fetch_user_details called for email: {email}")
    url = f"{ERPNEXT_API_URL}/api/resource/User/{email}"
    headers = {
        "Authorization": f"token {ERPNEXT_API_KEY}:{ERPNEXT_API_SECRET}",
        "Content-Type": "application/json",
    }
    print(f"Fetching user details from URL: {url}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        user_data = response.json().get('data')
        print(f"Successfully fetched user details: {user_data}")
        return user_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching user details for {email}: {e}")
        return None


def get_talk_username(full_name):
    """Converts the full name to a Nextcloud Talk username format."""
    print(f"get_talk_username called for full_name: {full_name}")
    talk_username = full_name.replace(" ", " ")
    print(f"Generated Talk username: {talk_username}")
    return talk_username


def create_talk_conversation(invite_username):
    """Creates a direct conversation on Nextcloud Talk with the given user using the v4 API."""
    print(f"create_talk_conversation called to invite: {invite_username}")
    db = get_db()

    # Check if we have a cached room_token for this user.
    cursor = db.execute("SELECT room_token FROM user_cache WHERE username = ?", (invite_username,))
    row = cursor.fetchone()
    if row:
        room_token = row['room_token']
        print(f"Found room_token in cache for {invite_username}: {room_token}")
        return room_token

    url = f"{NEXTCLOUD_API_URL}/ocs/v2.php/apps/spreed/api/v4/room?format=json"
    auth = (NEXTCLOUD_USERNAME, NEXTCLOUD_APP_PASSWORD)
    headers = {
        "OCS-APIRequest": "true",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    data = {
        "roomType": 1,  # 1 for direct conversation
        "invite": invite_username,
    }
    print(f"Creating Talk conversation with URL: {url}, data: {data}")
    retries = 0
    max_retries = 5  # Maximum number of retries
    backoff_delay = 1  # Initial backoff delay in seconds

    while retries < max_retries:
        try:
            response = requests.post(url, auth=auth, headers=headers, json=data)
            response.raise_for_status()
            room_data = response.json().get('ocs', {}).get('data')
            if room_data:
                room_token = room_data.get('token')
                print(f"Successfully created Talk conversation. Room token: {room_token}")
                # Cache the room_token in the database
                db.execute(
                    "INSERT OR REPLACE INTO user_cache (username, room_token) VALUES (?, ?)",
                    (invite_username, room_token),
                )
                db.commit()
                return room_token
            else:
                print(f"Failed to create Talk conversation with {invite_username}. Response: {response.text}")
                return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                retries += 1
                print(
                    f"Received 429 Too Many Requests. Retrying in {backoff_delay} seconds (Retry {retries}/{max_retries})")
                time.sleep(backoff_delay)
                backoff_delay *= 2  # Exponential backoff
            else:
                print(f"Error creating Talk conversation with {invite_username}: {e}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Error creating Talk conversation with {invite_username}: {e}")
            return None
    print(f"Max retries reached. Could not create Talk conversation with {invite_username}")
    return None


def send_talk_message(room_token, message):
    """Sends a message to the specified Nextcloud Talk room using the v1 API."""
    print(f"send_talk_message called for room_token: {room_token}, message: {message}")
    url = f"{NEXTCLOUD_API_URL}/ocs/v2.php/apps/spreed/api/v1/chat/{room_token}?format=json"
    auth = (NEXTCLOUD_USERNAME, NEXTCLOUD_APP_PASSWORD)
    headers = {
        "OCS-APIRequest": "true",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    data = {
        "message": message,
    }
    print(f"Sending message to Talk room with URL: {url}, data: {data}")
    try:
        response = requests.post(url, auth=auth, headers=headers, json=data)
        response.raise_for_status()
        print(f"Successfully sent message to Talk room: {room_token}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error sending message to Talk room {room_token}: {e}")
        return False


@app.route('/webhook', methods=['POST'])
def webhook_listener():
    """Listens for webhook events from ERPNext."""
    print("Webhook listener triggered.")
    data = None
    if request.is_json:
        data = request.get_json()
        print(f"Received JSON webhook data: {data}")
    elif request.form:
        print(f"Received URL-encoded webhook data: {request.form}")
        # Try to extract the JSON string from the keys
        for key in request.form.keys():
            try:
                data = json.loads(key)
                print(f"Parsed JSON from URL-encoded key: {data}")
                break  # Assuming the JSON is in the first key
            except json.JSONDecodeError:
                print("Could not decode JSON from URL-encoded key.")
                pass
    else:
        print("Received no data or unsupported data format.")
        return {"error": "No data or unsupported data format"}, 400

    if data:
        # Convert the data to a string representation for comparison
        data_str = json.dumps(data, sort_keys=True)
        db = get_db()
        # Check if the webhook payload has already been processed
        cursor = db.execute("SELECT 1 FROM processed_webhooks WHERE payload = ?", (data_str,))
        if cursor.fetchone():
            print(f"Webhook with payload {data_str} already processed. Ignoring.")
            return {"message": "Webhook already processed"}, 200

        # Mark the webhook as processed
        db.execute("INSERT INTO processed_webhooks (payload) VALUES (?)", (data_str,))
        db.commit()
        print(f"Processed webhooks: {data_str}")

        allocated_to_email = data.get('allocated_to')
        reference_type = data.get('reference_type')
        reference_name = data.get('reference_name')
        assigned_by_full_name = data.get('assigned_by_full_name')
        due_date = data.get('due_date')  # Get the due date
        # Construct the ERPNext document URL.  Assumes the reference_name is the document ID
        erpnext_doc_url = f"{ERPNEXT_API_URL.rstrip('/')}/app/{reference_type.lower()}/{reference_name}"

        if allocated_to_email:
            print(f"Extracted allocated_to_email: {allocated_to_email}")
            user_details = fetch_user_details(allocated_to_email)
            if user_details:
                full_name = user_details.get('full_name')
                if full_name:
                    print(f"Retrieved full_name: {full_name}")
                    talk_username = get_talk_username(full_name)
                    # Construct a more professional message, including the document link
                    notification_message = f"👋 Hey {full_name}, you’ve been assigned a new task!\n\n"
                    if due_date:
                        notification_message += f"*📅 Date:* {due_date}\n"
                    notification_message += f"*📌 Type:* {reference_type}\n"
                    notification_message += f"*🆔 Reference:* [{reference_name}]({erpnext_doc_url})\n" # Added link
                    notification_message += f"*👤 Assigned By:* {assigned_by_full_name}\n\n"
                    notification_message += "Please check it out and take the necessary action."
                    print(f"Notification message: {notification_message}")

                    room_token = create_talk_conversation(talk_username)
                    if room_token:
                        send_talk_message(room_token, notification_message)
                        return {"message": "Notification sent to Nextcloud Talk"}, 200
                    else:
                        print(f"Could not create or find Talk conversation for {talk_username}")
                        return {"error": "Could not create or find Talk conversation"}, 500
                else:
                    print(f"Could not retrieve full name for {allocated_to_email}")
                    return {"error": "Could not retrieve full name"}, 500
            else:
                print(f"Could not fetch user details for {allocated_to_email}")
                return {"error": f"Could not fetch user details for {allocated_to_email}"}, 500
        else:
            print("Webhook payload missing 'allocated_to' email.")
            return {"error": "Missing 'allocated_to' in webhook payload"}, 400
    else:
        return {"error": "No usable data received"}, 400


if __name__ == '__main__':
    print("Starting the ERPNext to Nextcloud Talk middleware...")
    # Initialize the database when the app starts
    with app.app_context():
        init_db()
    app.run(debug=True, port=5001, host='0.0.0.0')
