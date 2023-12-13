import mysql.connector

import os
from dotenv import load_dotenv
import mysql.connector

# Load environment variables from the .env file
load_dotenv()

# Retrieve the credentials from environment variables
host = str(os.getenv("HOST"))
database = str(os.getenv("DATABASE"))
user = str(os.getenv("USERDB"))
password = str(os.getenv("PASSWORD"))
port = 3306


# Create a connection to the MySQL server
try:
    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )

    if connection.is_connected():
        print("Connected to MySQL database")

        # You can perform database operations here

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Close the connection when done
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("Connection closed")
