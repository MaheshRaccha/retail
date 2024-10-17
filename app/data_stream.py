import requests
import time

url = 'http://localhost:5001/transaction'

def simulate_data_stream():
    while True:
        # Generate transaction data
        response = requests.get('http://localhost:5001/generate_data')
        transaction_data = response.json()

        # Send transaction data to Flask API
        requests.post(url, json=transaction_data)

        # Wait for a second before sending the next transaction
        time.sleep(1)

if __name__ == "__main__":
    simulate_data_stream()
