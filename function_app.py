import azure.functions as func
import logging
from dbClient import DBClient
import csv
import io

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
Last Modified Date: 2024-11-20

This function serves as an HTTP GET endpoint to test the database connection 
for the PowerTick API. It verifies if the API can successfully connect to the 
PostgreSQL database using the configured authentication method.

Authentication Methods:
- Local: Traditional username/password authentication (for local development).
- Cloud: Token-based authentication using Azure Managed Identity (for production).

Example:
Test the database connection locally:
curl -X GET "http://localhost:7071/api/testDBconnection"

Test the database connection in the cloud:
curl -X GET "https://powertick-api-py.azurewebsites.net/api/testDBconnection"

Response:
- Success: {'success': True, 'message': 'Connection to database successful.'}
- Failure: {'success': False, 'message': 'Connection to database failed: <error>'}
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
Last Modified Date: 2024-11-20

This function serves as an HTTP GET endpoint to generate and download all data 
from the `modbusrtu_commands` table in the PostgreSQL database as a CSV file.
 
The data is queried dynamically from the database, converted into CSV format 
in-memory, and returned as a downloadable file in the HTTP response.

Example:
Download the modbusrtu_commands table locally:
curl -O -J "http://localhost:7071/api/downloadModbusRTUcsv"

Download the modbusrtu_commands table in the cloud:
curl -O -J "https://powertick-api-py.azurewebsites.net/api/downloadModbusRTUcsv"
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
Author: Arturo Vargas Cuevas
Last Modified Date: 2024-11-21

Objective:
This function serves as an HTTP GET endpoint to generate a downloadable CSV file
containing all measurements for a specified powermeter, year, and month. It retrieves
the time zone of the powermeter from the database and uses it to filter measurements
based on the provided year and month. If no data exists for the given criteria, an
appropriate error message is returned.

Steps:
1. Retrieve the time zone of the powermeter from the `powermeters` table.
2. Filter the `measurements` table using the provided serial number (`sn`), year, and month.
3. Use the powermeter's time zone to accurately filter data.
4. Generate a CSV file dynamically and return it as a downloadable response.

Example:
Request measurements for serial number `DEMO0000001` for October 2024:
Local environment:
curl -O -J "http://localhost:7071/api/generateMeasurementsCSV?sn=DEMO0000001&year=2024&month=10"

Cloud environment:
curl -O -J "https://powertick-api-py.azurewebsites.net/api/generateMeasurementsCSV?sn=DEMO0000001&year=2024&month=10"
"""

@app.route(route="generateMeasurementsCSV", auth_level=func.AuthLevel.ANONYMOUS)
def generate_dev_measurements_csv(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing request to generate measurements CSV.")

    # Retrieve query parameters
    serial_number = req.params.get('sn')  # Changed to 'sn' as requested
    year = req.params.get('year')
    month = req.params.get('month')

    # Validate inputs
    if not serial_number or not year or not month:
        return func.HttpResponse(
            "Please provide 'sn' (serial number), 'year', and 'month' as query parameters.",
            status_code=400
        )

    try:
        # Initialize database client
        db_client = DBClient()
        connection = db_client.get_connection()
        cursor = connection.cursor()

        # Step 1: Retrieve the timezone for the serial number
        cursor.execute(
            """
            SELECT "time_zone"
            FROM "dev"."powermeters"
            WHERE "serial_number" = %s
            """,
            (serial_number,)
        )
        timezone_result = cursor.fetchone()

        if not timezone_result:
            return func.HttpResponse(
                f"Serial number '{serial_number}' not found in the database.",
                status_code=404
            )

        # Retrieve the timezone
        time_zone = timezone_result[0]

        # Step 2: Construct the date range for the specified year and month
        start_date = f"{year}-{month}-01 00:00:00"
        next_month = int(month) % 12 + 1
        next_month_year = int(year) + 1 if next_month == 1 else int(year)
        end_date = f"{next_month_year}-{next_month:02d}-01 00:00:00"

        # Step 3: Retrieve measurements with updated query
        cursor.execute(
            f"""
            SELECT 
              "timestamp" AT TIME ZONE %s AS timestamp_in_powermeter_time_zone,
              *
            FROM "dev"."measurements"
            WHERE "serial_number" = %s
              AND "timestamp" AT TIME ZONE %s >= %s
              AND "timestamp" AT TIME ZONE %s < %s
              AND "timestamp" < NOW()
            ORDER BY "timestamp" ASC
            """,
            (time_zone, serial_number, time_zone, start_date, time_zone, end_date)
        )
        rows = cursor.fetchall()

        if not rows:
            return func.HttpResponse(
                f"No measurements found for serial number '{serial_number}' in {year}-{month}.",
                status_code=404
            )

        # Step 4: Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Step 5: Generate CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(column_names)  # Write headers
        writer.writerows(rows)  # Write data rows

        # Close database connection
        db_client.close_connection()

        # Step 6: Create HTTP response with CSV file
        response = func.HttpResponse(
            body=output.getvalue(),
            mimetype="text/csv",
            status_code=200
        )
        response.headers["Content-Disposition"] = f"attachment; filename=measurements_{serial_number}_{year}_{month}.csv"
        return response

    except Exception as e:
        logging.error(f"Error generating measurements CSV: {e}")
        return func.HttpResponse(
            f"Failed to generate measurements CSV: {str(e)}",
            status_code=500
        )
    

@app.route(route="demoGenerateMeasurementsCSV", auth_level=func.AuthLevel.ANONYMOUS)
def generate_demo_measurements_csv(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing request to generate measurements CSV.")

    # Retrieve query parameters
    serial_number = req.params.get('sn')  # Changed to 'sn' as requested
    year = req.params.get('year')
    month = req.params.get('month')

    # Validate inputs
    if not serial_number or not year or not month:
        return func.HttpResponse(
            "Please provide 'sn' (serial number), 'year', and 'month' as query parameters.",
            status_code=400
        )

    try:
        # Initialize database client
        db_client = DBClient()
        connection = db_client.get_connection()
        cursor = connection.cursor()

        # Step 1: Retrieve the timezone for the serial number
        cursor.execute(
            """
            SELECT "time_zone"
            FROM "demo"."powermeters"
            WHERE "serial_number" = %s
            """,
            (serial_number,)
        )
        timezone_result = cursor.fetchone()

        if not timezone_result:
            return func.HttpResponse(
                f"Serial number '{serial_number}' not found in the database.",
                status_code=404
            )

        # Retrieve the timezone
        time_zone = timezone_result[0]

        # Step 2: Construct the date range for the specified year and month
        start_date = f"{year}-{month}-01 00:00:00"
        next_month = int(month) % 12 + 1
        next_month_year = int(year) + 1 if next_month == 1 else int(year)
        end_date = f"{next_month_year}-{next_month:02d}-01 00:00:00"

        # Step 3: Retrieve measurements with updated query
        cursor.execute(
            f"""
            SELECT 
              "timestamp" AT TIME ZONE %s AS timestamp_in_powermeter_time_zone,
              *
            FROM "demo"."measurements"
            WHERE "serial_number" = %s
              AND "timestamp" AT TIME ZONE %s >= %s
              AND "timestamp" AT TIME ZONE %s < %s
              AND "timestamp" < NOW()
            ORDER BY "timestamp" ASC
            """,
            (time_zone, serial_number, time_zone, start_date, time_zone, end_date)
        )
        rows = cursor.fetchall()

        if not rows:
            return func.HttpResponse(
                f"No measurements found for serial number '{serial_number}' in {year}-{month}.",
                status_code=404
            )

        # Step 4: Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Step 5: Generate CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(column_names)  # Write headers
        writer.writerows(rows)  # Write data rows

        # Close database connection
        db_client.close_connection()

        # Step 6: Create HTTP response with CSV file
        response = func.HttpResponse(
            body=output.getvalue(),
            mimetype="text/csv",
            status_code=200
        )
        response.headers["Content-Disposition"] = f"attachment; filename=measurements_{serial_number}_{year}_{month}.csv"
        return response

    except Exception as e:
        logging.error(f"Error generating measurements CSV: {e}")
        return func.HttpResponse(
            f"Failed to generate measurements CSV: {str(e)}",
            status_code=500
        )