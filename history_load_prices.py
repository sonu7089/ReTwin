import requests
import xml.etree.ElementTree as ET
import psycopg2
import psycopg2.extras # For batch execution
from psycopg2 import sql # For safe SQL query construction
from datetime import datetime, timezone, timedelta
import time
import sys # To exit gracefully on critical errors

# --- Configuration ---

# ENTSO-E API Configuration
ENTSOE_SECURITY_TOKEN = "4b1f3030-6326-4201-81fa-e49ae78f04a2" # Replace with your actual token if needed
ENTSOE_BASE_URL = "https://web-api.tp.entsoe.eu/api"
ENTSOE_NAMESPACE = {'ns': 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3'}
API_REQUEST_DELAY_SECONDS = 1 # Delay between API calls

# PostgreSQL Database Configuration (Replace with your actual details)
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "123",
    "host": "localhost", # Or your DB host
    "port": "5432"       # Default PostgreSQL port
}
DB_TABLE_NAME = "dayAheadPricesHist"

# Data Fetching Parameters
DEFAULT_IN_DOMAIN = "10Y1001A1001A82H" # Example: Germany/Luxembourg
DEFAULT_OUT_DOMAIN = "10Y1001A1001A82H"
DEFAULT_START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)
# Fetch data in chunks of this size
FETCH_INTERVAL_DAYS = 30

# --- Helper Functions ---

def build_entsoe_api_url(in_domain, out_domain, period_start_utc, period_end_utc):
    """Builds the ENTSO-E API URL for day-ahead prices (A44)."""
    params = {
        'securityToken': ENTSOE_SECURITY_TOKEN,
        'documentType': 'A44',        # Day-ahead prices document type
        'processType': 'A01',         # Day-ahead process type
        'in_Domain': in_domain,
        'out_Domain': out_domain,
        'periodStart': period_start_utc.strftime('%Y%m%d%H%M'), # Format YYYYMMDDHHMM
        'periodEnd': period_end_utc.strftime('%Y%m%d%H%M')      # Format YYYYMMDDHHMM
    }
    # Use requests' parameter handling for correct URL encoding
    req = requests.Request('GET', ENTSOE_BASE_URL, params=params)
    prepared_request = req.prepare()
    return prepared_request.url

def fetch_xml_data(url):
    """Fetches XML data from the specified URL."""
    try:
        response = requests.get(url, timeout=30) # Add a timeout
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        print(f"Successfully fetched data from {url}")
        return response.text
    except requests.exceptions.Timeout:
        print(f"Error: Request timed out for URL: {url}")
    except requests.exceptions.HTTPError as http_err:
        print(f"Error: HTTP error occurred: {http_err} - Status Code: {response.status_code}")
        # Optional: Check for specific status codes like 429 (Too Many Requests)
        if response.status_code == 429:
            print("Consider increasing API_REQUEST_DELAY_SECONDS.")
    except requests.exceptions.RequestException as req_err:
        print(f"Error: Failed to fetch data due to network issue: {req_err}")
    except Exception as e:
        print(f"Error: An unexpected error occurred during data fetch: {e}")
    return None

def parse_entsoe_xml(xml_content):
    """Parses the ENTSO-E XML content and extracts price data points."""
    extracted_data = []
    if not xml_content:
        print("Warning: XML content is empty, skipping parsing.")
        return extracted_data

    try:
        root = ET.fromstring(xml_content)

        # Iterate through each TimeSeries element
        for time_series in root.findall('ns:TimeSeries', ENTSOE_NAMESPACE):
            mrid_elem = time_series.find('ns:mRID', ENTSOE_NAMESPACE)
            mrid = None
            if mrid_elem is not None and mrid_elem.text:
                try:
                    # Validate and store the market region ID
                    mrid = int(mrid_elem.text)
                except ValueError:
                    print(f"Warning: Could not parse mRID '{mrid_elem.text}' as integer. Skipping TimeSeries.")
                    continue # Skip this TimeSeries if mRID is not a valid integer
            else:
                print("Warning: TimeSeries found without an mRID. Skipping.")
                continue # Skip TimeSeries without mRID

            # Process each Period within the TimeSeries
            for period in time_series.findall('ns:Period', ENTSOE_NAMESPACE):
                time_interval = period.find('ns:timeInterval', ENTSOE_NAMESPACE)
                if time_interval is None:
                    print(f"Warning: Period in TimeSeries {mrid} missing timeInterval. Skipping Period.")
                    continue

                period_start_elem = time_interval.find('ns:start', ENTSOE_NAMESPACE)
                period_end_elem = time_interval.find('ns:end', ENTSOE_NAMESPACE)

                if period_start_elem is None or period_end_elem is None:
                    print(f"Warning: Period in TimeSeries {mrid} missing start or end time. Skipping Period.")
                    continue

                period_start = period_start_elem.text
                period_end = period_end_elem.text

                # Extract each data point (price for a specific position/hour)
                for point in period.findall('ns:Point', ENTSOE_NAMESPACE):
                    position_elem = point.find('ns:position', ENTSOE_NAMESPACE)
                    price_amount_elem = point.find('ns:price.amount', ENTSOE_NAMESPACE)

                    if position_elem is not None and price_amount_elem is not None:
                        try:
                            position = int(position_elem.text)
                            price_amount = float(price_amount_elem.text) # Use float for price
                            extracted_data.append({
                                'time_series_mrid': mrid,
                                'period_start': period_start,
                                'period_end': period_end,
                                'position': position,
                                'price': price_amount
                            })
                        except ValueError:
                            print(f"Warning: Could not parse position or price for point in TimeSeries {mrid}. Skipping point.")
                    else:
                        print(f"Warning: Point missing position or price in TimeSeries {mrid}. Skipping point.")

    except ET.ParseError as e:
        print(f"Error: Failed to parse XML content: {e}")
    except Exception as e:
        print(f"Error: An unexpected error occurred during XML parsing: {e}")

    return extracted_data

# --- Database Functions ---

def connect_database():
    """Connects to the PostgreSQL database using configuration."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Successfully connected to the database.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"CRITICAL: Database connection failed: {e}")
        print("Please ensure PostgreSQL server is running and connection details are correct.")
        return None

def initialize_database_table(conn):
    """
    Ensures the target database table exists.
    WARNING: This function TRUNCATES (deletes all data from) the table before
             creating it if it doesn't exist. Remove TRUNCATE if you need
             to append data across multiple runs.
    """
    if not conn:
        return False

    table_identifier = sql.Identifier(DB_TABLE_NAME)
    try:
        with conn.cursor() as cur:
            # --- WARNING: Truncates the table on every script run ---
            # Remove this line if you want to append data incrementally.
            print(f"WARNING: Truncating table '{DB_TABLE_NAME}'...")
            cur.execute(sql.SQL("TRUNCATE TABLE {}").format(table_identifier))
            print(f"Table '{DB_TABLE_NAME}' truncated.")
            # --- End of TRUNCATE ---

            # Create table if it doesn't exist
            create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                time_series_mrid BIGINT NOT NULL,
                period_start TIMESTAMPTZ NOT NULL,
                period_end TIMESTAMPTZ NOT NULL,
                position INTEGER NOT NULL,
                price DECIMAL(10, 5) NOT NULL, -- Adjust precision/scale as needed
                fetched_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                -- Constraint to prevent duplicate entries for the same hour
                UNIQUE (time_series_mrid, period_start, position)
            );
            """).format(table_identifier)
            cur.execute(create_table_query)
            conn.commit()
            print(f"Table '{DB_TABLE_NAME}' is ready.")
            return True
    except psycopg2.Error as e:
        print(f"Error: Failed to initialize table '{DB_TABLE_NAME}': {e}")
        conn.rollback() # Rollback transaction on error
        return False

def insert_data_batch(conn, data):
    """Inserts a batch of data records into the database."""
    if not conn or not data:
        print("Info: No database connection or data provided for insertion.")
        return 0

    table_identifier = sql.Identifier(DB_TABLE_NAME)
    insert_query = sql.SQL("""
        INSERT INTO {} (time_series_mrid, period_start, period_end, position, price)
        VALUES (%s, %s, %s, %s, %s)
        -- If a duplicate is found (based on the UNIQUE constraint), do nothing.
        ON CONFLICT (time_series_mrid, period_start, position) DO NOTHING;
    """).format(table_identifier)

    # Prepare data tuples for batch insertion
    values_to_insert = [
        (
            item['time_series_mrid'],
            item['period_start'],
            item['period_end'],
            item['position'],
            item['price']
        ) for item in data
    ]

    inserted_count = 0
    try:
        with conn.cursor() as cur:
            # Use execute_batch for efficient insertion of multiple rows
            psycopg2.extras.execute_batch(cur, insert_query, values_to_insert, page_size=100)
            # Note: cur.rowcount after execute_batch with ON CONFLICT DO NOTHING
            # might not accurately reflect the number of *newly* inserted rows in all cases.
            # It often reflects the number of rows *processed*.
            inserted_count = cur.rowcount
            conn.commit()
            print(f"Successfully processed batch of {len(data)} records. (Actual inserts depends on conflicts)")
            return inserted_count # Return processed count as an indicator
    except psycopg2.Error as e:
        print(f"Error: Database error during batch insert: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error: An unexpected error occurred during data insertion: {e}")
        conn.rollback()
    return 0 # Return 0 if insertion failed

# --- Main Execution Logic ---

def main():
    """Main function to orchestrate the data fetching and storing process."""
    print("--- ENTSO-E Day-Ahead Price Data Fetcher ---")

    # Determine date range
    start_date = DEFAULT_START_DATE
    # Fetch up to the beginning of the current day in UTC
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    print(f"Fetching data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"  IN Domain: {DEFAULT_IN_DOMAIN}")
    print(f"  OUT Domain: {DEFAULT_OUT_DOMAIN}")
    print(f"  Output DB Table: {DB_TABLE_NAME}")
    print("-" * 30)

    # --- Database Setup ---
    db_conn = connect_database()
    if not db_conn:
        print("Exiting due to database connection failure.")
        sys.exit(1) # Exit script if DB connection fails

    if not initialize_database_table(db_conn):
        print("Exiting due to database table initialization failure.")
        db_conn.close()
        sys.exit(1) # Exit script if table setup fails

    # --- Data Fetching Loop ---
    current_start_date = start_date
    total_processed_count = 0 # Tracks rows processed by execute_batch

    while current_start_date < end_date:
        # Calculate end of the current fetch period
        current_end_date = current_start_date + timedelta(days=FETCH_INTERVAL_DAYS)
        # Ensure we don't fetch beyond the overall end date
        if current_end_date > end_date:
             current_end_date = end_date

        print(f"\nFetching data for period: {current_start_date.strftime('%Y-%m-%d')} to {current_end_date.strftime('%Y-%m-%d')}...")

        # 1. Build API URL
        api_url = build_entsoe_api_url(DEFAULT_IN_DOMAIN, DEFAULT_OUT_DOMAIN, current_start_date, current_end_date)
        print(f"  API URL: {api_url}")

        # 2. Fetch XML Data from API
        xml_data = fetch_xml_data(api_url)

        # 3. Parse XML Data
        if xml_data:
            price_data = parse_entsoe_xml(xml_data)
            if price_data:
                print(f"  Successfully parsed {len(price_data)} data points from XML.")
                # 4. Insert data into Database
                processed = insert_data_batch(db_conn, price_data)
                total_processed_count += processed
            else:
                print("  No valid price data extracted from the XML for this period.")
        else:
            print("  Failed to fetch or received empty data for this period. Skipping insertion.")

        # Move to the next period start date
        current_start_date = current_end_date # Start next fetch right after the previous one ended

        # Add a delay to avoid overwhelming the API
        print(f"Waiting for {API_REQUEST_DELAY_SECONDS} second(s)...")
        time.sleep(API_REQUEST_DELAY_SECONDS)

    # --- Cleanup ---
    if db_conn:
        db_conn.close()
        print("\nDatabase connection closed.")

    print(f"\n--- Script Finished ---")
    print(f"Data fetching complete up to {end_date.strftime('%Y-%m-%d')}.")
    print(f"Total records processed by database insertion: {total_processed_count}")
    print(f"Check the '{DB_TABLE_NAME}' table in the '{DB_CONFIG['dbname']}' database.")
    print("Note: 'Processed' count may include records skipped due to uniqueness constraints.")

if __name__ == "__main__":
    main()