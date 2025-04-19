import requests
import xml.etree.ElementTree as ET
import psycopg2 # Import PostgreSQL adapter
import psycopg2.extras # Import extras for batch execution
from psycopg2 import sql
from datetime import datetime, timezone, timedelta
import time # Import the time module for delays

# Define the namespace for easier parsing
NAMESPACE = {'ns': 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3'}

# --- Configuration ---
SECURITY_TOKEN = "4b1f3030-6326-4201-81fa-e49ae78f04a2" # Replace with your actual token if needed
BASE_URL = "https://web-api.tp.entsoe.eu/api"

# --- PostgreSQL Configuration --- (Replace with your actual details)
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "123"
DB_HOST = "localhost" # Or your DB host
DB_PORT = "5432"      # Default PostgreSQL port
DB_TABLE_NAME = "dayAheadPricesHist"

# --- Functions ---

def build_api_url(in_domain, out_domain, period_start_str, period_end_str):
    """Builds the ENTSO-E API URL with specified parameters."""
    params = {
        'securityToken': SECURITY_TOKEN,
        'documentType': 'A44',
        'processType': 'A01',
        'in_Domain': in_domain,
        'out_Domain': out_domain,
        'periodStart': period_start_str,
        'periodEnd': period_end_str
    }
    # Use requests' params handling to correctly encode the URL
    # This avoids manual string formatting issues
    req = requests.Request('GET', BASE_URL, params=params)
    prepared = req.prepare()
    return prepared.url

def fetch_data(url):
    """Fetches data from the specified URL."""
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        print(f"Successfully fetched data from {url}")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def parse_and_filter_xml(xml_content):
    """Parses the XML content, filters TimeSeries with odd mRIDs, and extracts data."""
    extracted_data = []
    if not xml_content:
        return extracted_data

    try:
        root = ET.fromstring(xml_content)

        for time_series in root.findall('ns:TimeSeries', NAMESPACE):
            mrid_elem = time_series.find('ns:mRID', NAMESPACE)
            if mrid_elem is not None and mrid_elem.text:
                try:
                    mrid = int(mrid_elem.text)
                    # Process all TimeSeries regardless of mRID
                    print(f"Processing TimeSeries with mRID: {mrid}") # Optional: Log processing

                    for period in time_series.findall('ns:Period', NAMESPACE):
                        time_interval = period.find('ns:timeInterval', NAMESPACE)
                        period_start = time_interval.find('ns:start', NAMESPACE).text
                        period_end = time_interval.find('ns:end', NAMESPACE).text

                        for point in period.findall('ns:Point', NAMESPACE):
                            position = point.find('ns:position', NAMESPACE).text
                            price_amount = point.find('ns:price.amount', NAMESPACE).text
                            extracted_data.append({
                                'TimeSeries_mRID': mrid,
                                'PeriodStart': period_start,
                                'PeriodEnd': period_end,
                                'Position': position,
                                'Price': price_amount
                            })
                except ValueError:
                    print(f"Warning: Could not parse mRID '{mrid_elem.text}' as integer. Skipping TimeSeries.")
                    continue # Skip this TimeSeries if mRID is not an integer
            else:
                print("Warning: TimeSeries found without an mRID. Skipping.")

    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during XML parsing: {e}")

    return extracted_data

# --- Database Functions ---

def connect_db():
    """Connects to the PostgreSQL database."""
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

def initialize_db_table(conn):
    """Creates the data table if it doesn't exist."""
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            # Truncate the table before inserting new data
            print(f"Truncating table '{DB_TABLE_NAME}'...")
            cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(DB_TABLE_NAME)))
            print(f"Table '{DB_TABLE_NAME}' truncated.")

            # Use sql module for safe table/identifier quoting
            create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                time_series_mrid BIGINT,
                period_start TIMESTAMPTZ,
                period_end TIMESTAMPTZ,
                position INTEGER,
                price DECIMAL(10, 5),
                fetched_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (time_series_mrid, period_start, position) -- Add constraint to avoid duplicates
            );
            """).format(sql.Identifier(DB_TABLE_NAME))
            cur.execute(create_table_query)
            conn.commit()
            print(f"Table '{DB_TABLE_NAME}' checked/created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating/checking table: {e}")
        conn.rollback() # Rollback in case of error

def insert_data_db(conn, data):
    """Inserts extracted data into the PostgreSQL database."""
    if not conn or not data:
        print("No connection or data to insert.")
        return 0

    inserted_count = 0
    skipped_count = 0
    try:
        with conn.cursor() as cur:
            # Use sql module for safe table/identifier quoting
            insert_query = sql.SQL("""
            INSERT INTO {} (time_series_mrid, period_start, period_end, position, price)
            VALUES (%s, %s, %s, %s, %s)
            """).format(sql.Identifier(DB_TABLE_NAME))

            # Prepare data for executemany
            values_to_insert = [
                (
                    item['TimeSeries_mRID'],
                    item['PeriodStart'],
                    item['PeriodEnd'],
                    item['Position'],
                    item['Price']
                ) for item in data
            ]

            # Use executemany for efficiency
            psycopg2.extras.execute_batch(cur, insert_query, values_to_insert)
            inserted_count = cur.rowcount # executemany doesn't reliably return count on conflict
            # We can't easily get the skipped count with ON CONFLICT DO NOTHING
            # A more complex query or checking existence first would be needed
            conn.commit()
            print(f"Attempted to insert {len(data)} records. Check DB for actual insertions (duplicates ignored).")

    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    except Exception as e:
        print(f"An unexpected error occurred during data insertion: {e}")
        conn.rollback()

    return inserted_count # Note: This count might not be accurate with ON CONFLICT

# --- Main Execution ---
if __name__ == "__main__":
    # --- Parameters --- 
    in_domain = "10Y1001A1001A82H" # Example: Germany/Luxembourg
    out_domain = "10Y1001A1001A82H"

    start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    # Fetch up to the beginning of today (UTC)
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    print("--- ENTSO-E Data Fetcher --- ")
    print(f"Fetching data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"  IN Domain: {in_domain}")
    print(f"  OUT Domain: {out_domain}")
    print(f"  Output DB Table: {DB_TABLE_NAME}")
    print("-----------------------------")

    # --- Database Setup ---
    db_conn = connect_db()
    if not db_conn:
        print("Exiting due to database connection failure.")
        exit(1)
    initialize_db_table(db_conn)

    # --- Data Fetching Loop ---
    current_date = start_date
    total_inserted_count = 0

    while current_date < end_date:
        period_start_dt = current_date
        period_end_dt = current_date + timedelta(days=30)

        period_start_str = period_start_dt.strftime('%Y%m%d%H%M')
        period_end_str = period_end_dt.strftime('%Y%m%d%H%M')

        print(f"\nFetching data for: {period_start_dt.strftime('%Y-%m-%d')}...")

        # 1. Build URL
        api_url = build_api_url(in_domain, out_domain, period_start_str, period_end_str)
        print(f"  URL: {api_url}")

        # 2. Fetch Data
        xml_data = fetch_data(api_url)

        # 3. Parse XML and Filter Data
        if xml_data:
            daily_data = parse_and_filter_xml(xml_data)
            if daily_data:
                print(f"  Successfully extracted {len(daily_data)} data points.")
                # 4. Insert data into DB
                inserted = insert_data_db(db_conn, daily_data)
                total_inserted_count += inserted # Accumulate count (may not be precise with ON CONFLICT)
            else:
                print("  No data extracted for this period.")
        else:
            print("  Failed to fetch data for this period.")

        # Move to the next day
        current_date += timedelta(days=30)

        # Add a small delay to be polite to the API
        time.sleep(1)

    # --- Cleanup ---
    if db_conn:
        db_conn.close()
        print("\nDatabase connection closed.")

    print(f"\n--- Script Finished ---")
    print(f"Attempted to insert data for the period. Check '{DB_TABLE_NAME}' table in '{DB_NAME}' database.")
    # print(f"Total records inserted/updated (approximate due to ON CONFLICT): {total_inserted_count}") # This count isn't reliable