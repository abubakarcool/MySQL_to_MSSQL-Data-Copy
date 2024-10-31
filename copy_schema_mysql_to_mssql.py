import pyodbc
import pymysql
from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy.dialects.mssql import INTEGER, VARCHAR, TEXT, DATETIME, TINYINT
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import mssql

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
mysql_metadata.reflect(bind=mysql_engine)  # Reflect all tables from MySQL
mysql_tables = mysql_metadata.tables

# SQL Server metadata
sqlserver_metadata = MetaData()

# Function to convert MySQL data types to SQL Server data types
def convert_to_sqlserver_type(mysql_column_type):
    """Convert MySQL column type to SQL Server column type using SQLAlchemy."""
    # Map MySQL types to SQL Server types
    type_mapping = {
        'INTEGER': INTEGER(),
        'VARCHAR': VARCHAR(length=255),  # Default length, adjust as necessary
        'TEXT': TEXT(),
        'TINYINT': TINYINT(),  # Used for boolean
        'DATETIME': DATETIME(),
        # Add more mappings as necessary
    }

    # If it's a type object, return it directly
    if isinstance(mysql_column_type, str):
        return type_mapping.get(mysql_column_type.upper(), VARCHAR(length=255))  # Default to VARCHAR

    return mysql_column_type

# Begin transferring tables from MySQL to SQL Server
for table_name, table in mysql_tables.items():
    print(f"Creating table {table_name} in SQL Server...")

    # Create new Table object for SQL Server
    sqlserver_table = Table(table_name, sqlserver_metadata)

    # Add columns to SQL Server table with converted types
    for col in table.columns:
        new_column = Column(
            col.name,
            convert_to_sqlserver_type(str(col.type)),
            primary_key=col.primary_key,
            nullable=col.nullable,
            default=col.default,
            autoincrement=col.autoincrement
        )
        sqlserver_table.append_column(new_column)

    # Generate SQL Server-compatible CREATE TABLE statement
    create_stmt = CreateTable(sqlserver_table).compile(dialect=mssql.dialect())
    sql_statement = str(create_stmt).replace("`", "")  # Remove backticks for SQL Server compatibility

    # Execute CREATE TABLE statement on SQL Server using text
    with mssql_engine.connect() as conn:
        conn.execute(text(sql_statement))
        conn.execute(text("COMMIT"))  # Explicit commit for each table creation

print("Table structure transfer complete!")
