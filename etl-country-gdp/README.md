
# ETL Pipeline for Country GDP Data

This project implements an **ETL (Extract, Transform, Load)** pipeline to process and analyze GDP data for countries. The pipeline extracts GDP data from a **Wikipedia page**, transforms it into a clean and structured format, and loads it into a **SQLite database** for querying and further analysis.

## Project Features

1. **Extraction**:
   - Scrapes GDP data from a Wikipedia page using `BeautifulSoup`.
   - Dynamically extracts data from the HTML table.

2. **Transformation**:
   - Cleans and formats GDP data.
   - Converts GDP values from USD (Millions) to USD (Billions).
   - Renames and standardizes column names.

3. **Loading**:
   - Saves the transformed data into a CSV file.
   - Loads the data into a SQLite database for querying.

4. **Querying**:
   - Allows executing SQL queries to analyze and filter data.

5. **Logging**:
   - Tracks the progress of the ETL pipeline using log files.

---

## Technologies and Libraries Used

- **Python**: Programming language.
- **BeautifulSoup**: For web scraping HTML content.
- **Requests**: For fetching the Wikipedia page.
- **Pandas**: For data manipulation and transformation.
- **NumPy**: For numerical operations.
- **SQLite3**: To store and query the processed data.

---

## File Structure

```plaintext
.
├── etl_pipeline.py          # Main Python script containing the ETL pipeline
├── etl_project_log.txt      # Log file generated during the ETL process
├── Countries_by_GDP.csv     # Output CSV file with transformed data
├── World_Economies.db       # SQLite database containing the loaded data
└── README.md                # Documentation for the project
```

---

## How to Run the Project

### Prerequisites
- Python 3.8 or higher installed on your system.
- Install the required libraries:
  ```bash
  pip install pandas numpy requests beautifulsoup4
  ```

### Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/shaik-noor/python-projects.git.git
   cd etl-country-gdp
   ```

2. Run the Python script:
   ```bash
   python etl_pipeline.py
   ```

3. View the results:
   - The transformed data will be saved in a file named `Countries_by_GDP.csv`.
   - The SQLite database `World_Economies.db` will contain the GDP data.

4. Query the database:
   - Open the SQLite database using your favorite SQLite viewer.
   - Run queries such as:
     ```sql
     SELECT * FROM Countries_by_GDP WHERE GDP_USD_billions >= 100;
     ```
