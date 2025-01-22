# Importing the required libraries
import requests
from bs4 import BeautifulSoup  # For extracting data from HTML
import pandas as pd  # For data manipulation and transformation
import numpy as np  # For numerical operations
import sqlite3  # For saving data into an SQLite database
from datetime import datetime  # For logging timestamps

# Define the main ETL functions
def extract(url, table_attribs):
    """
    Extracts GDP data from a Wikipedia page.
    Args:
        url (str): The URL of the Wikipedia page to scrape.
        table_attribs (list): Column names for the extracted table data.
    Returns:
        pd.DataFrame: A DataFrame containing the raw data.
    """
    log_progress("Starting data extraction...")
    response = requests.get(url)  # Fetch the webpage content
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
    
    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Initialize an empty DataFrame
    df = pd.DataFrame(columns=table_attribs)
    
    # Locate the correct table on the page
    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')  # Assuming the data is in the third table

    # Loop through the table rows and extract data
    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            if cols[0].find('a') is not None and '—' not in cols[2]:
                # Create a dictionary for each row
                data_dict = {
                    "Country": cols[0].a.contents[0],
                    "GDP_USD_millions": cols[2].contents[0]
                }
                # Append the row to the DataFrame
                df = pd.concat([df, pd.DataFrame(data_dict, index=[0])], ignore_index=True)

    log_progress("Data extraction completed.")
    return df


def transform(df):
    """
    Transforms the raw GDP data.
    - Converts GDP from string to float.
    - Changes GDP values from USD (Millions) to USD (Billions).
    Args:
        df (pd.DataFrame): The raw DataFrame to transform.
    Returns:
        pd.DataFrame: A cleaned and transformed DataFrame.
    """
    log_progress("Starting data transformation...")
    
    # Convert GDP values from string to float
    df["GDP_USD_millions"] = df["GDP_USD_millions"].str.replace(",", "").astype(float)
    
    # Convert GDP from Millions to Billions and rename the column
    df["GDP_USD_billions"] = np.round(df["GDP_USD_millions"] / 1000, 2)
    df.drop(columns=["GDP_USD_millions"], inplace=True)  # Drop the old column
    
    log_progress("Data transformation completed.")
    return df


def load_to_csv(df, csv_path):
    """
    Saves the transformed DataFrame to a CSV file.
    Args:
        df (pd.DataFrame): The DataFrame to save.
        csv_path (str): The file path for the CSV file.
    """
    df.to_csv(csv_path, index=False)
    log_progress(f"Data saved to CSV file at {csv_path}")


def load_to_db(df, sql_connection, table_name):
    """
    Loads the DataFrame into an SQLite database.
    Args:
        df (pd.DataFrame): The DataFrame to save.
        sql_connection (sqlite3.Connection): SQLite database connection.
        table_name (str): The name of the table to save the data.
    """
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress(f"Data loaded into database table: {table_name}")


def run_query(query_statement, sql_connection):
    """
    Executes a SQL query on the database.
    Args:
        query_statement (str): The SQL query to execute.
        sql_connection (sqlite3.Connection): SQLite database connection.
    Returns:
        pd.DataFrame: The query result as a DataFrame.
    """
    log_progress(f"Running query: {query_statement}")
    result = pd.read_sql_query(query_statement, sql_connection)
    log_progress("Query execution completed.")
    return result


def log_progress(message):
    """
    Logs the progress of the ETL pipeline to a file.
    Args:
        message (str): The message to log.
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Format the timestamp
    with open("etl_project_log.txt", "a") as log_file:
        log_file.write(f"{timestamp} - {message}\n")


# Main execution of the ETL pipeline
if __name__ == "__main__":
    # Configuration variables
    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    table_attribs = ["Country", "GDP_USD_millions"]
    db_name = 'World_Economies.db'
    table_name = 'Countries_by_GDP'
    csv_path = 'Countries_by_GDP.csv'

    try:
        log_progress("ETL pipeline started.")

        # Step 1: Extract data
        df_raw = extract(url, table_attribs)

        # Step 2: Transform data
        df_transformed = transform(df_raw)

        # Step 3: Load data to CSV
        load_to_csv(df_transformed, csv_path)

        # Step 4: Load data to SQLite database
        conn = sqlite3.connect(db_name)
        load_to_db(df_transformed, conn, table_name)

        # Step 5: Run a query on the database
        query = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100"
        query_result = run_query(query, conn)

        # Print the query result
        print(query_result)

        # Close the database connection
        conn.close()
        log_progress("ETL pipeline completed successfully.")

    except Exception as e:
        log_progress(f"ETL pipeline failed with error: {str(e)}")
        raise

