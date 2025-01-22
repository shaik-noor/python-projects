
# Bank ETL Project

## Project Overview

This project is a Python-based ETL (Extract, Transform, Load) pipeline designed to process and analyze data about the largest banks globally. The pipeline extracts data from a Wikipedia page, transforms it by converting market capitalization into multiple currencies, and then loads it into both a CSV file and a SQLite database. 

Additionally, the project performs queries on the database to demonstrate its utility.

---

## Features

1. **Extraction:**
   - Scrapes tabular data from a Wikipedia page about the largest banks.
   - Parses HTML using `BeautifulSoup` and converts it into a pandas DataFrame.

2. **Transformation:**
   - Converts market capitalization (MC) from USD into GBP, EUR, and INR using exchange rates from a CSV file.
   - Ensures data consistency and handles missing or invalid values.

3. **Load:**
   - Saves the processed data to a CSV file.
   - Stores the data in a SQLite database.

4. **Querying:**
   - Demonstrates SQL querying capabilities by running several queries on the database, such as fetching top 5 banks or calculating average market capitalization in GBP.

5. **Logging:**
   - Logs every step of the process to a log file for better traceability and debugging.

---

## Project Structure

```
.
├── bankEtl/
│   ├── code_log.txt               # Log file for the ETL process
│   ├── exchange_rate.csv          # Input CSV with exchange rates
│   ├── Largest_banks_data.csv     # Output CSV with transformed data
│   ├── Banks.db                   # SQLite database containing processed data
├── main.py                        # Main script containing ETL pipeline
├── README.md                      # Project documentation (this file)
```

---

## Requirements

- Python 3.7+
- Libraries: `requests`, `pandas`, `sqlite3`, `bs4`, `logging`

Install dependencies using:
```bash
pip install -r requirements.txt
```

---

## Usage

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/bank-etl.git
   cd bank-etl
   ```

2. Run the ETL pipeline:
   ```bash
   python main.py
   ```

3. Check the output:
   - Processed data will be available in `bankEtl/Largest_banks_data.csv`.
   - A SQLite database `Banks.db` will be created with the processed data.
   - Logs will be saved in `bankEtl/code_log.txt`.

---

## Key Functions

### 1. **Extract**
Extracts tabular data from the Wikipedia page using `requests` and `BeautifulSoup`.

### 2. **Transform**
Transforms the data by:
- Cleaning and ensuring data consistency.
- Calculating market capitalization in GBP, EUR, and INR based on exchange rates.

### 3. **Load**
Saves the transformed data to:
- A CSV file (`Largest_banks_data.csv`).
- A SQLite database (`Banks.db`).

### 4. **Query**
Demonstrates SQL querying:
- Fetch all data from the database.
- Calculate average market capitalization in GBP.
- Fetch names of the top 5 banks.

---

## Example Queries

- **Print the entire table:**
  ```sql
  SELECT * FROM Largest_banks;
  ```

- **Calculate the average market capitalization in GBP:**
  ```sql
  SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
  ```

- **Fetch the names of the top 5 banks:**
  ```sql
  SELECT Name FROM Largest_banks LIMIT 5;
  ```

---

## Logs

The script logs all activities in `bankEtl/code_log.txt`, including:
- Start and completion of each ETL step.
- Key transformation details.
- Errors, if any occur.

