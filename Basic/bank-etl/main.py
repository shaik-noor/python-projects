import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
import logging
from io import StringIO

# Log File Setup
LOG_FILE = "./code_log.txt"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(message)s')

# -------------------------------------
# Global Constants / Configuration
# -------------------------------------

DATA_URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
EXCHANGE_RATE_CSV_PATH = "./exchange_rate.csv"
OUTPUT_CSV_PATH = "./Largest_banks_data.csv"
DB_NAME = "./Banks.db"
TABLE_NAME = "Largest_banks"

# -------------------------------------
# Task 1: Loggin Function
# -------------------------------------
def log_progress(message):
    """Logs the progress of the code. """
    logging.info(message)

# -------------------------------------
# Task 2: Extract Function
# -------------------------------------
def extract():
    """extract tabuler data from the URL."""
    log_progress("Starting the extraction from the URL.")
    response = requests.get(DATA_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find_all("table", {"class": "wikitable"})

    # read HTML table into a DataFrame
    table_html = str(table)
    tables = pd.read_html(StringIO(table_html))
    print(f"Number of tables present {len(tables)}")
    # df = pd.read_html(StringIO(table_html))[1]

    # Safely handle table selection
    if len(tables) >= 1:
        # Accessing the second table in the html
        df = tables[0]
        df = df.iloc[:, [1, 2]]  # Assuming the desired columns are the 2nd and 3rd
        df.columns = ["Name", "MC_USD_Billion"]
    else:
        raise ValueError("No tables found or insufficient tables to extract data.")
    
    log_progress("Data extraction completed.")
    return df

# -------------------------------------
# Task 3: Transform Function
# -------------------------------------

def transform(df, exchange_rates_path):
    """Transform the dataframe with market capitalization in multiple currencies."""
    log_progress("Starting the data transformation.")

    # load the exchange rates data from the CSV
    exchange_rates = pd.read_csv(exchange_rates_path)
    rates = exchange_rates.set_index("Currency").to_dict()["Rate"]
    
    # Ensure MC_USD_Billion is numeric
    df["MC_USD_Billion"] = pd.to_numeric(df["MC_USD_Billion"], errors="coerce")
    

    # add the transformed columns
    df["MC_GBP_Billion"] = (df["MC_USD_Billion"] * rates["GBP"]).round(2)
    df["MC_EUR_Billion"] = (df["MC_USD_Billion"] * rates["EUR"]).round(2)
    df["MC_INR_Billion"] = (df["MC_USD_Billion"] * rates["INR"]).round(2)

    log_progress("Data transformation completed.")

    # Print the marketcap of the 5th largest bank in the EUR
    print(f"Market capitalization of the 5th largest bank (EUR): {df['MC_EUR_Billion'][4]}")
    log_progress(f"MC_EUR_Billion[4]: {df['MC_EUR_Billion'][4]}")
    return df

# -------------------------------------
# Task 4: Load to CSV Function
# -------------------------------------

def load_to_csv(df, output_path):
    """save the dataframe to the csv file."""
    log_progress("Started the data load to the CSV.")
    df.to_csv(output_path, index=False)
    log_progress(f"Data successfully dave to {output_path}.")

# -------------------------------------
# Task 5: Load to Sqlite Database Function
# -------------------------------------

def load_to_db(df, db_name, table_name):
    """Load the dataframe into the sqlite database."""
    log_progress("started loading the data to the database.")
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    log_progress("Data successfully loaded to the sqlite database.")

# -------------------------------------
# Task 6: Query the Database Function
# -------------------------------------

def run_queries(conn, query):
    """Run the SQL query on the database and print the results."""
    log_progress(f"Executing query: {query}")
    print(f"Query: {query}")
    result = pd.read_sql_query(query, conn)
    print(result)
    return result

# Main Executions

if __name__ == "__main__":
    try:
        # task 2: Extract
        df_extracted = extract()

        # tack 3: Transform
        df_transformed = transform(df_extracted, EXCHANGE_RATE_CSV_PATH)

        # task 4: Load to the CSV
        load_to_csv(df_transformed, OUTPUT_CSV_PATH)

        # task 5: load to the database
        load_to_db(df_transformed, DB_NAME, TABLE_NAME)

        # Task 6: Query the database
        conn = sqlite3.connect(DB_NAME)

        # Query 1: Print the entire table
        run_queries(conn, f"SELECT * FROM {TABLE_NAME}")

        # Query 2: Print the average market capitalization in GBP
        run_queries(conn, f"SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}")

        # Query 3: Print the names of the top 5 banks
        run_queries(conn, f"SELECT Name FROM {TABLE_NAME} LIMIT 5")

        conn.close()

        log_progress("All tasks completed successfully.")

    except Exception as e:
        log_progress(f"An error occured: {e}")
        print(f"An error occured {e}")