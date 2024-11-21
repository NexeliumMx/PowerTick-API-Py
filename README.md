
# PowerTick Python API Documentation

## Overview
The PowerTick Python API is built with Azure Functions and provides endpoints to interact with Modbus RTU commands and database testing. This documentation outlines all available endpoints, their purposes, and how to interact with them.

---

## API Endpoints

### Public API Endpoints

| **Endpoint**                | **Method** | **Description**                                          |
|-----------------------------|------------|----------------------------------------------------------|
| `/api/downloadModbusRTUcsv` | GET        | Download the `modbusrtu_commands` table as a CSV file.   |

### Test API Endpoints

| **Endpoint**            | **Method** | **Description**                                         |
|-------------------------|------------|---------------------------------------------------------|
| `/api/http_trigger`     | GET, POST  | Sample trigger for testing.                             |
| `/api/testDBconnection` | GET        | Test the database connection.                           |

---

## Setup Instructions

### Running the API Locally

1. **Clone the repository**:
   ```bash
   git clone https://github.com/NexeliumMx/PowerTick-API-Py.git
   cd PowerTick-API-Py/
   ```

2. **Create a virtual environment and activate it**:
   ```bash
   python -m venv env
   source env/bin/activate   # On Windows, use `env\Scripts\activate`
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Start the Azure Functions runtime**:
   ```bash
   func start
   ```

5. **Use VSCode for debugging and development**:
   - Open the project folder in VSCode.
   - Install the "Azure Functions" and "Python" extensions.
   - Set up the debugging configuration for Azure Functions (done automatically with the extensions).

---

## Usage Examples

Below are examples of how to interact with each API endpoint using `curl`.

### `/api/downloadModbusRTUcsv` - GET

**Description**: Download the `modbusrtu_commands` table as a CSV file.

**Examples**:

- **Download locally**:
  ```bash
  curl -O -J "http://localhost:7071/api/downloadModbusRTUcsv"
  ```

- **Download from the cloud**:
  ```bash
  curl -O -J "https://powertick-api-py.azurewebsites.net/api/downloadModbusRTUcsv"
  ```

---

### `/api/http_trigger` - GET, POST

**Description**: Sample trigger for testing.

**Examples**:

- **Invoke the sample trigger locally**:
  ```bash
  curl -X GET "http://localhost:7071/api/http_trigger"
  ```

- **Invoke the sample trigger in the cloud**:
  ```bash
  curl -X GET "https://powertick-api-py.azurewebsites.net/api/http_trigger"
  ```

---

### `/api/testDBconnection` - GET

**Description**: Test the database connection.

**Examples**:

- **Test the database connection locally**:
  ```bash
  curl -X GET "http://localhost:7071/api/testDBconnection"
  ```

- **Test the database connection in the cloud**:
  ```bash
  curl -X GET "https://powertick-api-py.azurewebsites.net/api/testDBconnection"
  ```

---

Feel free to modify the example parameters as needed for testing purposes.
