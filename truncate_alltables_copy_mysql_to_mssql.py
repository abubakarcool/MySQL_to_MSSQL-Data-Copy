import pyodbc
import pymysql
from sqlalchemy import create_engine, MetaData, Table, select, text, func
from sqlalchemy.exc import SQLAlchemyError, DataError
import time
from decimal import Decimal

# SQL Server connection (using pyodbc)
mssql_conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=ipack-svr-rpt;DATABASE=Report_1;'
    'Trusted_Connection=yes;'
)

# MySQL connection (using pymysql)
mysql_conn = pymysql.connect(
    host='sever_address',
    user='test123',
    password='test123',
    db='hotpack_test'
)

# Create SQLAlchemy engines with existing connections
mysql_engine = create_engine('mysql+pymysql://', creator=lambda: mysql_conn)
mssql_engine = create_engine('mssql+pyodbc://', creator=lambda: mssql_conn)

# MySQL metadata reflection
mysql_metadata = MetaData()
mysql_metadata.reflect(bind=mysql_engine)
mysql_tables = mysql_metadata.tables

# SQL Server metadata reflection
mssql_metadata = MetaData()
mssql_metadata.reflect(bind=mssql_engine)

# Loop through each table in MySQL
for table_name, mysql_table in mysql_tables.items():
    print(f"\nStarting to process table: {table_name}")
    start_time = time.time()

    # Get the SQL Server table metadata
    mssql_table = mssql_metadata.tables.get(table_name)

    # Check if both tables exist
    if mysql_table is not None and mssql_table is not None:
        # Get the total number of rows in the MySQL table for progress tracking
        with mysql_engine.connect() as mysql_conn:
            total_rows = mysql_conn.execute(select(func.count()).select_from(mysql_table)).scalar()
            print(f"Total rows to copy from MySQL table `{table_name}`: {total_rows}")

        # Skip tables with no rows
        if total_rows == 0:
            print(f"No data to copy for table `{table_name}`. Skipping...")
            continue

        # Step 1: Truncate the table in SQL Server
        with mssql_engine.connect() as conn:
            print(f"Truncating table {table_name} in SQL Server...")
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            conn.execute(text("COMMIT"))

        # Step 2: Copy data from MySQL to SQL Server
        print(f"Starting data copy for table `{table_name}`...")

        # Get column lengths from SQL Server metadata
        column_max_lengths = {col.name: col.type.length for col in mssql_table.columns if hasattr(col.type, 'length')}

        with mysql_engine.connect() as mysql_conn:
            # Fetch all rows from the MySQL table at once
            mysql_data = mysql_conn.execute(select(mysql_table)).fetchall()

            # Prepare insert data, truncating specified columns if needed and excluding rows with NULLs in non-nullable columns
            insert_data = []
            for row in mysql_data:
                insert_row = {}
                skip_row = False
                for mssql_col in mssql_table.columns:
                    value = row._mapping.get(mssql_col.name)

                    # Check if the column has a maximum length constraint and needs truncation
                    max_length = column_max_lengths.get(mssql_col.name)
                    if max_length and isinstance(value, str) and len(value) > max_length:
                        print(f"Truncating column '{mssql_col.name}' in table `{table_name}` from {len(value)} to {max_length} characters.")
                        value = value[:max_length]  # Truncate to SQL Server column max length

                    # Handle Decimal values explicitly by converting them to a string with fixed precision
                    if isinstance(value, Decimal):
                        value = f"{value:.2f}"  # Format to two decimal places as string

                    # Check if column is non-nullable and value is None
                    if not mssql_col.nullable and value is None:
                        skip_row = True  # Flag to skip this row if it has NULL in a non-nullable column
                        break

                    insert_row[mssql_col.name] = value

                if not skip_row:  # Only add rows that pass the non-nullable check
                    insert_data.append(insert_row)

            # Insert all rows at once into SQL Server table
            with mssql_engine.connect() as conn:
                try:
                    conn.execute(mssql_table.insert(), insert_data)
                    conn.execute(text("COMMIT"))
                    print(f"Table `{table_name}`: All {len(insert_data)} rows copied successfully.")
                except DataError as e:
                    print(f"Data error copying data for table `{table_name}`: {e}")
                except SQLAlchemyError as e:
                    print(f"Error copying data for table `{table_name}`: {e}")

        # Calculate and display the time taken for this table
        end_time = time.time()
        time_taken = end_time - start_time
        print(f"Completed copying table `{table_name}`. Time taken: {time_taken:.2f} seconds.")

    else:
        print(f"Table `{table_name}` does not exist in one of the databases and will be skipped.")

print("\nData transfer for all tables from MySQL to SQL Server is complete!")
