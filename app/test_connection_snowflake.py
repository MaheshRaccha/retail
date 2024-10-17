import snowflake.connector

def connect_to_snowflake():
    conn = snowflake.connector.connect(
    user='maheshrachamalla',
    password='Raccha@2024',
    account='DD60020.azure.southcentralus',
    warehouse='my_warehouse',
    database='retail',
    schema='my_schema',
    login_timeout=120, # Increase the timeout value if needed
    insecure_mode=True
)
    
    print("Connected to Snowflake!")
    return conn

if __name__ == "__main__":
    connect_to_snowflake()
