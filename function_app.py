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