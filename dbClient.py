import os
import psycopg2
from azure.identity import DefaultAzureCredential

class DBClient:
    def __init__(self):
        self.environment = os.getenv("ENVIRONMENT", "local")
        self.connection = None

    def get_connection(self):
        if self.connection:
            return self.connection

        if self.environment == "local":
            # Local environment using username and password
            print("Running locally. Using traditional user/password authentication.")
            self.connection = psycopg2.connect(
                host=os.getenv("PGHOST"),
                dbname=os.getenv("PGDATABASE"),
                user=os.getenv("PGUSER"),
                password=os.getenv("PGPASSWORD"),
                port=os.getenv("PGPORT", 5432),
                sslmode="require"  # Enforce SSL for secure connection
            )
        elif self.environment == "cloud":
            # Azure environment using token-based authentication with Managed Identity
            print("Running in Azure. Using token-based authentication with Managed Identity.")
            try:
                # Use DefaultAzureCredential to obtain the token
                credential = DefaultAzureCredential()
                token = credential.get_token("https://ossrdbms-aad.database.windows.net").token

                self.connection = psycopg2.connect(
                    host=os.getenv("PGHOST"),
                    dbname=os.getenv("PGDATABASE"),
                    user=os.getenv("PGUSER", "PowerTick-API-Py"),  # Managed Identity name as the user
                    password=token,  # Use the token as the password
                    port=os.getenv("PGPORT", 5432),
                    sslmode="require"  # Enforce SSL for secure connection
                )
            except Exception as e:
                print(f"Error connecting to database in cloud environment: {str(e)}")
                raise e
        else:
            raise ValueError(f"Unknown environment: {self.environment}. Cannot establish database connection.")
        
        return self.connection

    def close_connection(self):
        if self.connection:
            self.connection.close()
            self.connection = None