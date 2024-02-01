# BankMarketCapETL

## Introduction

The BankMarketCapETL project is designed to create a robust ETL (Extract, Transform, Load) pipeline focusing on the market capitalization of the world's largest banks. This pipeline extracts data from a specified Wikipedia page, transforms the market capitalization values according to exchange rates, and loads the data into both a SQLite database and a CSV file for further analysis or visualization. This project aims to provide financial analysts and enthusiasts with up-to-date information on bank valuations in various currencies, facilitating global financial comparisons and analyses.

## Resources

- **Data Source URL**: [List of largest banks on Wikipedia](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks)
- **Exchange Rate CSV Path**: Path to a CSV file containing the latest exchange rates.
- **SQLite Database**: `Banks.db` for storing transformed data.
- **Output CSV File**: `Largest_banks_data.csv` for easy access and sharing of the transformed data.

## Steps in the ETL Process

### 1. Data Extraction

- **Objective**: Extract the list of the world's largest banks and their market capitalization in USD from the specified Wikipedia page.
- **Method**: Use `BeautifulSoup` to parse the HTML content of the page and extract relevant data into a pandas DataFrame.

### 2. Data Transformation

- **Objective**: Convert the market capitalization values from USD to GBP, EUR, and INR using the exchange rates provided in the `exchange_rate.csv` file.
- **Method**: Read the exchange rates from the CSV file into a dictionary and apply these rates to the `MC_USD_Billion` column in the DataFrame. The transformed data will include new columns: `MC_GBP_Billion`, `MC_EUR_Billion`, and `MC_INR_Billion`, with values rounded to the nearest billion.

### 3. Data Loading to CSV

- **Objective**: Save the transformed DataFrame to a CSV file for easy access and distribution.
- **Method**: Use the `to_csv` method of pandas DataFrame to write the data to `Largest_banks_data.csv`, ensuring the data is accessible outside the Python environment.

### 4. Data Loading to Database

- **Objective**: Load the transformed data into a SQLite database for persistent storage and query capabilities.
- **Method**: Utilize `sqlite3` and pandas' `to_sql` function to insert the DataFrame into the `Largest_banks` table in the `Banks.db` database.

### 5. Query Execution

- **Objective**: Demonstrate the ability to run queries against the loaded data in the SQLite database.
- **Method**: Implement a `run_queries` function to execute SQL queries, printing both the query and its results. Sample queries include selecting the entire table, calculating the average market capitalization, and listing the top 5 banks by market capitalization.

![Query Execution](https://github.com/adiimated/BankMarketCapETL/blob/main/Task_6_SQL.png)

*Fig.1: Execution of SQL queries

## Conclusion

The BankMarketCapETL project streamlines the process of gathering, converting, and storing critical financial data regarding the world's largest banks. By automating the extraction of up-to-date market capitalization data and accommodating currency conversions, this pipeline serves as a valuable tool for financial analysis and reporting.
