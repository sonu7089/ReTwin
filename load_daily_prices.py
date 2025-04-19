import requests
import xml.etree.ElementTree as ET
import psycopg2
import psycopg2.extras  # For batch execution
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection # For type hinting
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any

# --- Constants ---
# ENTSO-E API Configuration
# NOTE: Consider using environment variables or a config file for sensitive data like tokens/passwords.
SECURITY_TOKEN = "4b1f3030-6326-4201-81fa-e49ae78f04a2" # Replace with your actual token
BASE_URL = "https://web-api.tp.entsoe.eu/api"
DOCUMENT_TYPE = 'A44' # Day Ahead Prices
PROCESS_TYPE = 'A01' # Daily process

# PostgreSQL Database Configuration (Replace with your actual details)
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "123"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_TABLE_NAME = "dayAheadPricesDaily"

# XML Parsing Configuration
NAMESPACE = {'ns': 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3'}
TIME_SERIES_TAG = 'ns:TimeSeries'
MRID_TAG = 'ns:mRID'
PERIOD_TAG = 'ns:Period'
TIME_INTERVAL_TAG = 'ns:timeInterval'
START_TAG = 'ns:start'
END_TAG = 'ns:end'
POINT_TAG = 'ns:Point'
POSITION_TAG = 'ns:position'
PRICE_AMOUNT_TAG = 'ns:price.amount'


# --- API Functions ---

def build_api_url(in_domain: str, out_domain: str, period_start_str: str, period_end_str: str) -> str:
    """Builds the ENTSO-E API URL with specified parameters."""
    params = {
        'securityToken': SECURITY_TOKEN,
        'documentType': DOCUMENT_TYPE,
        'processType': PROCESS_TYPE,
        'in_Domain': in_domain,
        'out_Domain': out_domain,
        'periodStart': period_start_str,
        'periodEnd': period_end_str
    }
    req = requests.Request('GET', BASE_URL, params=params)
    prepared = req.prepare()
    return prepared.url

def fetch_data(url: str) -> Optional[str]:
    """Fetches data from the specified URL, returning the text content or None on error."""
    try:
        response = requests.get(url)
        response.raise_for_status() # Raises HTTPError for 4xx/5xx status codes
        print(f"Successfully fetched data from {url}")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

def parse_xml_data(xml_content: str) -> List[Dict[str, Any]]:
    """Parses the XML content and extracts relevant price data points."""
    extracted_data: List[Dict[str, Any]] = []
    if not xml_content:
        return extracted_data

    try:
        root = ET.fromstring(xml_content)

        for time_series in root.findall(TIME_SERIES_TAG, NAMESPACE):
            mrid_elem = time_series.find(MRID_TAG, NAMESPACE)
            if mrid_elem is None or not mrid_elem.text:
                print("Warning: TimeSeries found without an mRID. Skipping.")
                continue

            try:
                mrid = int(mrid_elem.text)
            except ValueError:
                print(f"Warning: Could not parse mRID '{mrid_elem.text}' as integer. Skipping TimeSeries.")
                continue

            # Process all points within this TimeSeries
            for period in time_series.findall(PERIOD_TAG, NAMESPACE):
                time_interval = period.find(TIME_INTERVAL_TAG, NAMESPACE)
                if time_interval is None: continue # Skip if no time interval

                period_start_elem = time_interval.find(START_TAG, NAMESPACE)
                period_end_elem = time_interval.find(END_TAG, NAMESPACE)
                if period_start_elem is None or period_end_elem is None: continue # Skip if interval incomplete

                period_start = period_start_elem.text
                period_end = period_end_elem.text

                for point in period.findall(POINT_TAG, NAMESPACE):
                    position_elem = point.find(POSITION_TAG, NAMESPACE)
                    price_amount_elem = point.find(PRICE_AMOUNT_TAG, NAMESPACE)

                    if position_elem is not None and price_amount_elem is not None:
                        extracted_data.append({
                            'TimeSeries_mRID': mrid,
                            'PeriodStart': period_start,
                            'PeriodEnd': period_end,
                            'Position': position_elem.text,
                            'Price': price_amount_elem.text
                        })

    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
    except Exception as e:
        # Catch unexpected errors during parsing
        print(f"An unexpected error occurred during XML parsing: {e}")

    return extracted_data

# --- Database Functions ---

def connect_db() -> Optional[PgConnection]:
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Successfully connected to the database.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error connecting to database: {e}")
        print("Please ensure PostgreSQL is running and connection details are correct.")
        return None

def initialize_db(conn: PgConnection):
    """Creates the data table if it doesn't already exist."""
    if not conn: return

    create_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {} (
        id SERIAL PRIMARY KEY,
        time_series_mrid BIGINT,
        period_start TIMESTAMPTZ,
        period_end TIMESTAMPTZ,
        position INTEGER,
        price DECIMAL(10, 5),
        fetched_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        -- Ensures that we don't insert the exact same data point multiple times
        -- if the script runs again without TRUNCATE for some reason.
        UNIQUE (time_series_mrid, period_start, position)
    );
    """).format(sql.Identifier(DB_TABLE_NAME))

    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print(f"Table '{DB_TABLE_NAME}' checked/created successfully.")
    except psycopg2.Error as e:
        print(f"Error ensuring table '{DB_TABLE_NAME}' exists: {e}")
        conn.rollback() # Rollback any partial changes

def insert_data(conn: PgConnection, data: List[Dict[str, Any]]):
    """Clears the existing daily data and inserts the new data batch."""
    if not conn or not data:
        print("No database connection or data provided for insertion.")
        return

    inserted_count = 0
    table_identifier = sql.Identifier(DB_TABLE_NAME)

    try:
        with conn.cursor() as cur:
            # Clear previous day's data before inserting the new batch
            print(f"Truncating table '{DB_TABLE_NAME}'...")
            cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY").format(table_identifier)) # RESTART IDENTITY resets sequence
            print(f"Table '{DB_TABLE_NAME}' truncated.")

            # Prepare data for batch insertion
            insert_query = sql.SQL("""
            INSERT INTO {} (time_series_mrid, period_start, period_end, position, price)
            VALUES (%s, %s, %s, %s, %s)
            """).format(table_identifier)

            values_to_insert = [
                (
                    item['TimeSeries_mRID'],
                    item['PeriodStart'],
                    item['PeriodEnd'],
                    int(item['Position']), # Ensure position is integer
                    float(item['Price'])   # Ensure price is float/decimal
                ) for item in data
            ]

            # Execute batch insert
            psycopg2.extras.execute_batch(cur, insert_query, values_to_insert, page_size=100) # Adjust page_size if needed
            inserted_count = len(data) # execute_batch doesn't return accurate count easily
            conn.commit()
            print(f"Successfully inserted {inserted_count} records into '{DB_TABLE_NAME}'.")

    except (psycopg2.Error, ValueError, TypeError) as e: # Catch DB errors and potential type conversion errors
        print(f"Error inserting data into '{DB_TABLE_NAME}': {e}")
        conn.rollback() # Rollback the transaction on error
    except Exception as e:
        print(f"An unexpected error occurred during data insertion into '{DB_TABLE_NAME}': {e}")
        conn.rollback()


# --- Main Execution ---
def main():
    """Main function to orchestrate the data fetching and storing process."""
    # --- Define Parameters ---
    # Example: Germany/Luxembourg bidding zone
    in_domain = "10Y1001A1001A82H"
    out_domain = "10Y1001A1001A82H"

    # Calculate the date range for the *previous* full day in UTC
    # The ENTSO-E API typically requires the period to cover whole days (midnight to midnight)
    # and expects UTC timestamps.
    today_start_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start_utc = today_start_utc - timedelta(days=1)
    # The end period for ENTSO-E is typically exclusive (00:00 of the next day)
    yesterday_end_utc = today_start_utc

    # Format dates for the API (YYYYMMDDHHMM)
    period_start_str = yesterday_start_utc.strftime('%Y%m%d%H%M')
    period_end_str = yesterday_end_utc.strftime('%Y%m%d%H%M')

    print("--- ENTSO-E Daily Price Fetcher --- ")
    print(f"Fetching data for day: {yesterday_start_utc.strftime('%Y-%m-%d')}")
    print(f"Period Start (UTC):   {period_start_str}")
    print(f"Period End (UTC):     {period_end_str}")
    print(f"IN Domain:            {in_domain}")
    print(f"OUT Domain:           {out_domain}")
    print(f"Output DB Table:      {DB_TABLE_NAME}")
    print("----------------------------------")

    db_conn = None # Initialize connection variable
    try:
        # --- Database Setup ---
        db_conn = connect_db()
        if not db_conn:
            print("Exiting: Database connection failed.")
            return # Exit main function if DB connection fails

        initialize_db(db_conn) # Ensure the table exists

        # --- Data Fetching and Processing ---
        print(f"\nFetching data...")
        api_url = build_api_url(in_domain, out_domain, period_start_str, period_end_str)
        print(f"API URL: {api_url}")

        xml_data = fetch_data(api_url)

        if xml_data:
            daily_data = parse_xml_data(xml_data)
            if daily_data:
                print(f"Successfully extracted {len(daily_data)} data points.")
                # Insert data into the database
                insert_data(db_conn, daily_data)
            else:
                print("No data points extracted from the XML.")
        else:
            print("Failed to fetch data for this period.")

    except Exception as e:
        # Catch any unexpected errors during the main execution flow
        print(f"An unexpected error occurred in the main process: {e}")
    finally:
        # --- Cleanup ---
        if db_conn:
            db_conn.close()
            print("\nDatabase connection closed.")

    print(f"\n--- Daily Script Finished ---")
    print(f"Check '{DB_TABLE_NAME}' table in '{DB_NAME}' database for data from {yesterday_start_utc.strftime('%Y-%m-%d')}.")


if __name__ == "__main__":
    main()