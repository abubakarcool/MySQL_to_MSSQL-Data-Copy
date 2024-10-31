import pyodbc
import pymysql
from sqlalchemy import create_engine, MetaData, Table, select, text, func
from sqlalchemy.exc import SQLAlchemyError

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
mysql_metadata.reflect(bind=mysql_engine)  # Reflect all tables from MySQL
mysql_tables = mysql_metadata.tables

# SQL Server metadata reflection
mssql_metadata = MetaData()
mssql_metadata.reflect(bind=mssql_engine)  # Reflect all tables from SQL Server

# Define the maximum length for the `remarks` column to avoid truncation errors
MAX_REMARKS_LENGTH = 255  # Adjust based on SQL Server's `remarks` column length

table_name = 'assign_defect_printing'
print(f"Processing table {table_name}...")

# Get SQL Server and MySQL table metadata
mssql_table = mssql_metadata.tables.get(table_name)
mysql_table = mysql_tables.get(table_name)

if mysql_table is not None and mssql_table is not None:
    # Get the total number of rows in the MySQL table for progress tracking
    with mysql_engine.connect() as mysql_conn:
        total_rows = mysql_conn.execute(select(func.count()).select_from(mysql_table)).scalar()
        print(f"Total rows to copy from MySQL table : {total_rows}")

    # Step 1: Truncate the table in SQL Server
    with mssql_engine.connect() as conn:
        print(f"Truncating table {table_name} in SQL Server...")
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        conn.execute(text("COMMIT"))

    # Step 2: Copy data from MySQL to SQL Server with progress tracking
    copied_rows = 0  # Initialize counter for tracking copied rows
    batch_size = 100  # Define batch size for copying rows in chunks

    with mysql_engine.connect() as mysql_conn:
        # Fetch rows in batches from MySQL
        for offset in range(0, total_rows, batch_size):
            mysql_data = mysql_conn.execute(select(mysql_table).offset(offset).limit(batch_size)).fetchall()

            # Prepare insert data, truncating `remarks` if needed
            insert_data = []
            for row in mysql_data:
                insert_row = {}
                for mssql_col in mssql_table.columns:
                    value = row._mapping.get(mssql_col.name)
                    if mssql_col.name == 'remarks' and isinstance(value, str):
                        # Truncate the `remarks` field if it exceeds the maximum length
                        insert_row[mssql_col.name] = value[:MAX_REMARKS_LENGTH]
                    else:
                        insert_row[mssql_col.name] = value
                insert_data.append(insert_row)

            # Insert batch into SQL Server table
            if insert_data:
                with mssql_engine.connect() as conn:
                    try:
                        conn.execute(mssql_table.insert(), insert_data)
                        conn.execute(text("COMMIT"))
                        # Update and display progress
                        copied_rows += len(insert_data)
                        progress = (copied_rows / total_rows) * 100
                        print(f"Progress: {copied_rows}/{total_rows} rows copied ({progress:.2f}%)")
                    except SQLAlchemyError as e:
                        print(f"Error copying data in batch: {e}")
else:
    print(f"Table {table_name} does not exist in one of the databases.")

print("Data transfer for the table from MySQL to SQL Server is complete!")
