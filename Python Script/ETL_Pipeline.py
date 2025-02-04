import pandas as pd
import mysql.connector
import logging

# For logging configuration
logging.basicConfig(
    filename= 'ETL_Pipeline.log',  
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Uploading of CSV files
CSV_FILES = [
    r"D:\ETL_Project\IT_Services_Marketshare_2019.csv",
    r"D:\ETL_Project\IT_Services_Marketshare_2020.csv",
    r"D:\ETL_Project\IT_Services_Marketshare_2021.csv",
    r"D:\ETL_Project\IT_Services_Marketshare_2022.csv",
    r"D:\ETL_Project\IT_Services_Marketshare_2023.csv"
]

# MySQL connection details
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="market_data"
)

DB_TABLE = "market_data"
logging.info('Connected to MySQL successfully')

#This function is to retrieve table columns from the database
def get_table_columns(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return columns

# Function to process each chunk of data and insert it into MySQL
def process_chunk(chunk, table_name, connection, table_columns):
    try:
        # Standardization of column names
        chunk.columns = [col.strip().replace(" - ", "_").replace(" ", "_").replace("-", "_") for col in chunk.columns]

        # Rename of specific columns to match SQL table schema
        chunk.rename(columns={
            'Super_Region': 'Super_Region',
            'HQ_Country': 'HQ_Country',
            'VendorRevenue_USD': 'VendorRevenue_USD',
            'ConstantCurrency_Revenue_USD': 'ConstantCurrency_Revenue_USD',
        }, inplace=True)

        # For removal of 'Yr' from Year col
        if 'Year' in chunk.columns:
            chunk['Year'] = chunk['Year'].astype(str).str.replace(' YR', '', regex=False).astype(int)

        # Replacing missing values
        chunk.fillna("Unknown", inplace=True)

        # Transformation of currency fields to numeric values
        currency_columns = ["VendorRevenue_USD", "ConstantCurrency_Revenue_USD"]
        for col in currency_columns:
            if col in chunk.columns:
                chunk[col] = chunk[col].replace({r'\$': '', ',': '', ' ': ''}, regex=True)
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0)

        # For the alignmwnt of columns with the database schema
        chunk = chunk[[col for col in chunk.columns if col in table_columns]]

        # Inserting the data into MySQL
        cursor = connection.cursor()
        cols = ", ".join(chunk.columns)
        placeholders = ", ".join(["%s"] * len(chunk.columns))
        insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

        # Inserting all rows in the chunk
        cursor.executemany(insert_query, chunk.values.tolist())
        connection.commit()
        cursor.close()

        logging.info(f"{len(chunk)} rows processed and inserted successfully.")

    except Exception as e:
        logging.error(f"Error processing chunk: {e}")

table_columns = get_table_columns(mydb, DB_TABLE)


for file in CSV_FILES:
    try:
        logging.info(f"Processing file: {file}")
        chunksize = 100000  
        for chunk in pd.read_csv(file, chunksize=chunksize):
            process_chunk(chunk, DB_TABLE, mydb, table_columns)
        logging.info(f"Finished processing file: {file}")
    except Exception as e:
        logging.error(f"Error processing file {file}: {e}")

mydb.close()
logging.info("Database connection closed.")
