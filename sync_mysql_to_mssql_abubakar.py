import pymysql
import pyodbc
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MySQL connection details
mysql_conn = pymysql.connect(
    host='sever_address',
    user='test123',
    password='test123',
    db='hotpack_test'
)

# MSSQL connection details
mssql_conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=ipack-svr-aatdb\\mes_kosta;"  # Double backslashes for escaping
    "DATABASE=database_mssql_name;"
    "UID=test;"
    "PWD=test;"
)

# Establish connections
mssql_conn = pyodbc.connect(mssql_conn_str)
mssql_cursor = mssql_conn.cursor()

def fetch_mysql_data():
    try:
        logging.info("Fetching data from MySQL...")
        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute("SELECT id, first_name, last_name, user_name, password, role, status, email FROM user_tb")
        data = mysql_cursor.fetchall()
        mysql_cursor.close()  # Close MySQL cursor after use
        logging.info(f"Fetched {len(data)} rows from MySQL.")
        
        # Convert data to tuples for hashable comparison
        return [tuple(row) for row in data]
    except Exception as e:
        logging.error(f"Error fetching data from MySQL: {e}")
        return []

def count_mssql_records():
    try:
        logging.info("Counting records in MSSQL...")
        mssql_cursor.execute("SELECT COUNT(*) FROM dbo.user_tb")
        count = mssql_cursor.fetchone()[0]
        logging.info(f"Found {count} records in MSSQL.")
        return count
    except Exception as e:
        logging.error(f"Error counting records in MSSQL: {e}")
        return 0

def compare_and_sync_data(mysql_data):
    try:
        # Get the current record count in MSSQL
        initial_mssql_count = count_mssql_records()

        # Fetch existing data from MSSQL to compare
        logging.info("Fetching data from MSSQL for comparison...")
        mssql_cursor.execute("SELECT id, first_name, last_name, user_name, password, role, status, email FROM dbo.user_tb")
        mssql_data = mssql_cursor.fetchall()

        # Convert MSSQL data to tuples for comparison
        mssql_data_tuples = [tuple(row) for row in mssql_data]

        # Convert data to sets for comparison
        mysql_set = set(mysql_data)
        mssql_set = set(mssql_data_tuples)

        identical_records = mysql_set.intersection(mssql_set)
        new_records = mysql_set - mssql_set

        logging.info(f"Identical records between MySQL and MSSQL: {len(identical_records)}")
        logging.info(f"New records to be inserted into MSSQL: {len(new_records)}")

        # Insert new records into MSSQL
        if new_records:
            insert_query = """
            INSERT INTO dbo.user_tb (id, first_name, last_name, user_name, password, role, status, email) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            for row in new_records:
                mssql_cursor.execute(insert_query, row)
            mssql_conn.commit()
            logging.info(f"Inserted {len(new_records)} new records into MSSQL.")
        else:
            logging.info("No new records to insert into MSSQL.")

        # Final count of records in MSSQL after the sync
        final_mssql_count = count_mssql_records()
        logging.info(f"Final record count in MSSQL after synchronization: {final_mssql_count}")

    except Exception as e:
        logging.error(f"Error during synchronization: {e}")

def main():
    logging.info("Synchronization process started.")
    
    # Fetch data from MySQL
    mysql_data = fetch_mysql_data()
    
    if mysql_data:
        compare_and_sync_data(mysql_data)
    else:
        logging.warning("No data fetched from MySQL. Skipping synchronization.")

if __name__ == "__main__":
    try:
        main()
    finally:
        # Ensure connections are closed even if an error occurs
        mysql_conn.close()
        mssql_conn.close()
        logging.info("Connections closed. Script finished.")
