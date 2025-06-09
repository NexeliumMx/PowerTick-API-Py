import azure.functions as func
import logging
from dbClient import DBClient
import csv
import io
import json
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

"""
Author: Arturo Vargas Cuevas
Date: 2024-11-20
Brief: This function serves as an HTTP GET endpoint to test the database connection 

Copyright (c) 2025 BY: Nexelium Technological Solutions S.A. de C.V.
All rights reserved.
"""

@app.route(route="testDBconnection", auth_level=func.AuthLevel.ANONYMOUS)
def testDBconnection(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing database connection test request.')

    # Create an instance of DBClient
    db_client = DBClient()

    # Function to connect to the database and test the connection
    def connect_to_database():
        try:
            # Get a database connection
            connection = db_client.get_connection()

            # Test connection by opening and closing immediately
            connection.close()
            db_client.close_connection()
            return {"success": True, "message": "Connection to database successful."}

        except Exception as e:
            return {"success": False, "message": f"Connection to database failed: {str(e)}"}

    # Call the function to test the connection
    result = connect_to_database()

    # Return the result as a JSON response
    return func.HttpResponse(
        body=str(result),
        mimetype="application/json",
        status_code=200
    )



"""
Author: Arturo Vargas Cuevas
Date: 2024-11-20

Copyright (c) 2025 BY: Nexelium Technological Solutions S.A. de C.V.
All rights reserved.
"""

@app.route(route="downloadModbusRTUcsv", auth_level=func.AuthLevel.ANONYMOUS)
def downloadModbusRTUcsv(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing request to download modbusrtu_commands table as CSV.")

    # Initialize database client
    db_client = DBClient()

    try:
        # Establish database connection
        connection = db_client.get_connection()
        cursor = connection.cursor()

        # Query all data from the modbusrtu_commands table
        cursor.execute("SELECT * FROM public.modbusrtu_commands")
        rows = cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Generate CSV in-memory
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(column_names)  # Write headers
        writer.writerows(rows)  # Write data rows

        # Close database connection
        db_client.close_connection()

        # Create HTTP response with CSV file
        response = func.HttpResponse(
            body=output.getvalue(),
            mimetype="text/csv",
            status_code=200,
        )
        response.headers["Content-Disposition"] = "attachment; filename=modbusrtu_commands.csv"
        return response

    except Exception as e:
        logging.error(f"Error downloading CSV: {e}")
        return func.HttpResponse(
            body=f"Failed to generate CSV: {str(e)}",
            status_code=500
        )
    
"""
Author: Arturo Vargas
Endpoint: GET /api/generateMeasurementsCSV
Brief: This function generates a CSV file containing measurements for a specific power meter identified by its serial number (sn) for a given year and month. The CSV file includes all measurements recorded in the specified time range, adjusted to the power meter's timezone
Date: 2025-06-07

Copyright (c) 2025 BY: Nexelium Technological Solutions S.A. de C.V.
All rights reserved.
"""
@app.route(route="generateMeasurementsCSV", auth_level=func.AuthLevel.ANONYMOUS)
def generate_dev_measurements_csv(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing request to generate measurements CSV (reworked endpoint).")

    # --- Parse and Validate Params ---
    user_id = req.params.get('user_id')
    powermeter_id = req.params.get('powermeter_id')
    start_utc = req.params.get('start_utc')
    end_utc = req.params.get('end_utc')
    enviroment = req.params.get('enviroment', 'production')

    # Allowed environments
    ALLOWED_ENVIROMENTS = ['production', 'demo', 'dev']
    if enviroment not in ALLOWED_ENVIROMENTS:
        return func.HttpResponse(
            "Invalid enviroment parameter. Allowed: production, demo, dev.",
            status_code=400
        )

    # Schema selection
    schema = 'public'
    if enviroment == 'demo':
        schema = 'demo'
    elif enviroment == 'dev':
        schema = 'dev'

    # Validate required parameters
    if not user_id or not powermeter_id or not start_utc or not end_utc:
        return func.HttpResponse(
            "Missing required parameter: user_id, powermeter_id, start_utc, end_utc are required.",
            status_code=400
        )

    try:
        db_client = DBClient()
        connection = db_client.get_connection()
        cursor = connection.cursor()

        # --- Build SQL Query ---
        sql = f'''
            WITH authorized_powermeter AS (
                SELECT p.powermeter_id, p.time_zone
                FROM {schema}.powermeters p
                JOIN public.user_installations ui ON p.installation_id = ui.installation_id
                WHERE ui.user_id = %s
                  AND p.powermeter_id = %s
            ),
            powermeter_info AS (
                SELECT
                    powermeter_id,
                    time_zone,
                    EXTRACT(hour FROM ('1970-01-01 00:00:00' AT TIME ZONE time_zone)
                        - '1970-01-01 00:00:00'::timestamp) AS offset_hours
                FROM authorized_powermeter
            ),
            time_window AS (
                SELECT
                    powermeter_id,
                    time_zone,
                    (%s AT TIME ZONE time_zone) AT TIME ZONE 'UTC' AS start_utc,
                    (%s AT TIME ZONE time_zone) AT TIME ZONE 'UTC' AS end_utc
                FROM powermeter_info
            )
            SELECT
                m.*
            FROM {schema}.measurements m
            JOIN time_window tw ON m.powermeter_id = tw.powermeter_id
            WHERE m."timestamp" >= tw.start_utc
              AND m."timestamp" <  tw.end_utc
              AND m."timestamp" <= NOW()
            ORDER BY m."timestamp" ASC;
        '''
        params = [user_id, powermeter_id, start_utc, end_utc]
        cursor.execute(sql, params)
        rows = cursor.fetchall()

        if not rows:
            return func.HttpResponse(
                f"No measurements found for powermeter_id '{powermeter_id}' in the specified range.",
                status_code=404
            )

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Generate CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(column_names)
        writer.writerows(rows)

        db_client.close_connection()

        # Create HTTP response with CSV file
        response = func.HttpResponse(
            body=output.getvalue(),
            mimetype="text/csv",
            status_code=200
        )
        response.headers["Content-Disposition"] = f"attachment; filename=measurements_{powermeter_id}_{start_utc}_{end_utc}.csv"
        return response

    except Exception as e:
        logging.error(f"Error generating measurements CSV: {e}")
        return func.HttpResponse(
            f"Failed to generate measurements CSV: {str(e)}",
            status_code=500
        )