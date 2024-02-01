# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
table_name = 'Largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
exchange_rate_csv_path = './exchange_rate.csv'
output_csv_path = './Largest_banks_data.csv'


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    log_message = f"{timestamp} : {message}\n"
    with open("code_log.txt", "a") as log_file:
        log_file.write(log_message)

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    data_list = []
    for row in rows[1:]:
        cols = row.find_all('td')
        if len(cols) > 2:
            bank_name = cols[1].text.strip()
            market_cap_raw = cols[2].text.strip().replace('\n', '').replace(',', '').replace('B', '').replace('$', '')
            try:
                market_cap = float(market_cap_raw)
            except ValueError:
                market_cap = None
            data_dict = {'Name': bank_name, 'MC_USD_Billion': market_cap}
            data_list.append(data_dict)

    df = pd.DataFrame(data_list, columns=table_attribs)
    return df

def transform(df, csv_path):
    ''' This function reads the exchange rate CSV file, converts its contents to a dictionary,
    and scales the MC_USD_Billion column by the corresponding exchange rate factor for GBP, EUR, and INR.'''
    exchange_rates_df = pd.read_csv(csv_path)
    
    exchange_rates = exchange_rates_df.set_index('Currency').to_dict()['Rate']
    
    df['MC_GBP_Billion'] = [np.round(x * exchange_rates['GBP'], 2) if x is not None else None for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rates['EUR'], 2) if x is not None else None for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rates['INR'], 2) if x is not None else None for x in df['MC_USD_Billion']]
    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, conn, if_exists='replace', index=False)

import pandas as pd

def run_queries(query_statement, sql_connection):
    ''' This function runs the provided SQL query on the database connected by sql_connection
    and prints the query statement along with its results. '''
    print(f"Executing Query: {query_statement}")
    df = pd.read_sql_query(query_statement, sql_connection)
    print(df)
    log_progress(f"Executed query: {query_statement}")

log_progress('Initiating ETL process')
df = extract(url, table_attribs)
print(df)

df_transformed = transform(df, exchange_rate_csv_path)
print("Market capitalization of the 5th largest bank in billion EUR:", df_transformed['MC_EUR_Billion'][4])

log_progress("Data transformation complete. Initiating Loading process")
load_to_csv(df_transformed, output_csv_path)

conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")
load_to_db(df_transformed, conn, table_name)
conn.close()
log_progress("Database connection closed")


conn = sqlite3.connect(db_name)
query1 = "SELECT * FROM Largest_banks"
run_queries(query1, conn)
query2 = "SELECT AVG(MC_GBP_Billion) AS Avg_Market_Cap_GBP FROM Largest_banks"
run_queries(query2, conn)
query3 = "SELECT Name FROM Largest_banks LIMIT 5"
run_queries(query3, conn)
conn.close()
log_progress("Database connection closed")

