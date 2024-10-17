from google.cloud import pubsub_v1
import snowflake.connector
import json
from google.oauth2 import service_account

# Snowflake connection details
def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user='maheshrachamalla',
        password='Raccha@2024',
        account='DD60020.azure.southcentralus',
        warehouse='my_warehouse',
        database='retail',
        schema='my_schema'
    )
    return conn

# Initialize Pub/Sub subscriber client
def subscribe_to_pubsub_and_process():
    service_account_path = "C:\\Users\\Mahes\\Documents\\GitHub\\sa-keys\\mahesh-raccha-gcp-pubsub-sa-key.json"
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path('mahesh-raccha-gcp', 'retail_transactions-sub')

    # Define the callback function to handle messages
    def callback(message):
        print(f"Received message: {message.data}")
        transaction_data = json.loads(message.data.decode('utf-8'))

        # Connect to Snowflake
        conn = connect_to_snowflake()
        cursor = conn.cursor()

        # Insert the transaction data into Snowflake
        insert_query = f"""
            INSERT INTO retail_transactions (TransactionID, CustomerID, Store, Timestamp, EventType, ProductID, Amount, PaymentMethod, ProductCategory)
            VALUES ({transaction_data['TransactionID']}, {transaction_data['CustomerID']}, '{transaction_data['Store']}', 
                    '{transaction_data['Timestamp']}', '{transaction_data['EventType']}', {transaction_data['ProductID']}, 
                    {transaction_data['Amount']}, '{transaction_data['PaymentMethod']}', '{transaction_data['ProductCategory']}')
        """
        cursor.execute(insert_query)
        conn.commit()

        # Acknowledge the message
        message.ack()
        print("Message processed and acknowledged")

    # Subscribe to the subscription
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    # Block the main thread to keep the subscriber running
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Subscription cancelled")

if __name__ == "__main__":
    subscribe_to_pubsub_and_process()
