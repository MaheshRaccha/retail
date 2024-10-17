from google.cloud import pubsub_v1
from google.oauth2 import service_account
from flask import Flask, request, jsonify
import json
import time
import random

app = Flask(__name__)

service_account_path = "C:\\Users\\Mahes\\Documents\\GitHub\\sa-keys\\mahesh-raccha-gcp-pubsub-sa-key.json"

# Load the credentials from the service account key file
credentials = service_account.Credentials.from_service_account_file(service_account_path)


# Initialize Pub/Sub client

publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path('mahesh-raccha-gcp', 'retail_transactions')

# Root route (Optional)
@app.route('/')
def home():
    return "Welcome to the Retail Transaction API!"

# Endpoint to handle transaction data and publish to Pub/Sub
@app.route('/transaction', methods=['POST'])
def transaction():
    data = request.json
    
    # Publish the received transaction to Pub/Sub
    data_str = json.dumps(data)  # Convert the transaction to a JSON string
    data_bytes = data_str.encode('utf-8')  # Encode the data as bytes
    
    # Publish to Pub/Sub
    future = publisher.publish(topic_path, data_bytes)
    print(f"Published message ID: {future.result()}")

    print("Received Transaction: ", data)
    return jsonify({"status": "success", "message": "Transaction received and published"}), 200

# Endpoint to generate random transaction data and publish to Pub/Sub
@app.route('/generate_data', methods=['GET'])
def generate_data():
    # Simulate transaction data
    transaction_data = {
        "CustomerID": random.randint(1000, 9999),
        "Store": random.choice(["online", "instore"]),
        "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        "TransactionID": random.randint(10000, 99999),
        "EventType": random.choice(["click", "purchase"]),
        "ProductID": random.randint(100, 500),
        "Amount": round(random.uniform(10.0, 500.0), 2),
        "PaymentMethod": random.choice(["Credit Card", "Debit Card", "PayPal", "Cash"]),
        "ProductCategory": random.choice(["Electronics", "Groceries", "Clothing", "Health"]),
    }

    # Convert to JSON and publish the transaction data to Pub/Sub
    data_str = json.dumps(transaction_data)
    data_bytes = data_str.encode('utf-8')

    future = publisher.publish(topic_path, data_bytes)
    print(f"Published message ID: {future.result()}")

    return jsonify(transaction_data), 200

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5001)
