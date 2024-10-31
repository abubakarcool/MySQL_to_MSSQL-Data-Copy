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
    host='IPACK-SVR-DEP',
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

# Tables to process
tables_to_process = ['inquiry_bom_update', 'pack_weight_info']

# Loop through each specified table
for table_name in tables_to_process:
    print(f"\nStarting to process table: {table_name}")
    start_time = time.time()

    # Get MySQL and SQL Server table metadata
    mysql_table = mysql_tables.get(table_name)
    mssql_table = mssql_metadata.tables.get(table_name)

    if mysql_table is not None and mssql_table is not None:
        # Fetch total rows from MySQL for progress tracking
        with mysql_engine.connect() as mysql_conn:
            total_rows = mysql_conn.execute(select(func.count()).select_from(mysql_table)).scalar()
            print(f"Total rows to copy from MySQL table `{table_name}`: {total_rows}")

        # Truncate the table in SQL Server before copying data
        with mssql_engine.connect() as conn:
            print(f"Truncating table {table_name} in SQL Server...")
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            conn.execute(text("COMMIT"))

        # Fetch SQL Server column lengths
        column_max_lengths = {col.name: col.type.length for col in mssql_table.columns if hasattr(col.type, 'length')}
        print(f"Column max lengths for `{table_name}` in SQL Server: {column_max_lengths}")

        # Fetch data from MySQL and prepare for insertion into SQL Server
        with mysql_engine.connect() as mysql_conn:
            mysql_data = mysql_conn.execute(select(mysql_table)).fetchall()
            insert_data = []

            for row in mysql_data:
                insert_row = {}
                for mssql_col in mssql_table.columns:
                    value = row._mapping.get(mssql_col.name)

                    # Apply UTF-8 encoding and truncation for strings if necessary
                    max_length = column_max_lengths.get(mssql_col.name)
                    if isinstance(value, str):
                        value = value.encode('utf-8', errors='ignore').decode('utf-8')
                        if max_length and len(value) > max_length:
                            print(f"Truncating `{mssql_col.name}` in `{table_name}`: Original length = {len(value)}, Max length = {max_length}")
                            value = value[:max_length]

                    # Handle Decimal values explicitly
                    if isinstance(value, Decimal):
                        # Convert to a formatted string with fixed precision (e.g., 10, 2 for two decimal places)
                        value = f"{value:.2f}"

                    # Check for None values in non-nullable columns
                    if not mssql_col.nullable and value is None:
                        print(f"Skipping row due to NULL in non-nullable column `{mssql_col.name}`.")
                        break

                    insert_row[mssql_col.name] = value
                insert_data.append(insert_row)

            # Insert data into SQL Server and catch any errors
            with mssql_engine.connect() as conn:
                try:
                    conn.execute(mssql_table.insert(), insert_data)
                    conn.execute(text("COMMIT"))
                    print(f"Table `{table_name}`: All rows copied successfully.")
                except DataError as e:
                    print(f"Data error copying data for table `{table_name}`: {e}")
                    print(f"Problematic row data: {insert_data[-1]}")
                except SQLAlchemyError as e:
                    print(f"Error copying data for table `{table_name}`: {e}")

        end_time = time.time()
        time_taken = end_time - start_time
        print(f"Completed copying table `{table_name}`. Time taken: {time_taken:.2f} seconds.")

    else:
        print(f"Table `{table_name}` does not exist in one of the databases and will be skipped.")

print("\nData transfer for specified tables from MySQL to SQL Server is complete!")
