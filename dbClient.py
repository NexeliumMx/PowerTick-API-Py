"""
Author: Arturo Vargas Cuevas
Last Modified Date: 2024-11-21

This module provides a database client for connecting to a PostgreSQL database. 
It supports two environments:
- **Local Development**: Uses traditional username/password authentication.
- **Azure Environment**: Implements token-based authentication with Azure Managed Identity.

Key Features:
- Handles dynamic environment detection (`local` or `cloud`) to determine the authentication method.
- Uses `DefaultAzureCredential` to retrieve authentication tokens for Azure.
- Provides methods to establish (`get_connection`) and close (`close_connection`) database connections.
- Ensures secure connections by enforcing SSL for all environments.

Conditions for the Code to Work:
- Environment variables (`PGHOST`, `PGDATABASE`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `ENVIRONMENT`) must be correctly set.
- For Azure, the Function App must have a System-Assigned Managed Identity enabled and access permissions to the PostgreSQL server.
- A valid token must be retrievable from Azure Managed Identity in the cloud environment.

Usage:
- Create an instance of the `DBClient` class.
- Call `get_connection` to obtain a database connection.
- Always call `close_connection` after completing database operations to release resources.
"""

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