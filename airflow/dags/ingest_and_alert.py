import sqlite3
import datetime
import requests
import io
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 3, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(
    'price_catcher_etl-v2',
    default_args=default_args,
    schedule_interval='@daily'
)

# db connection
conn = sqlite3.connect('DOSM.db')
c = conn.cursor()

#  alert function
def alert(text):
    token = "6078066145:AAEBXCfKpPRZXQh1wkDPwa6hEMZBywXsqbE"
    chat_id = "-1001944537404"
    url_req = "https://api.telegram.org/bot" + token + "/sendmessage" + "?chat_id=" + chat_id + "&text=" + text

    sendtext = requests.get(url_req)
    print(sendtext.json())
    


def ingest_pricecatcher_data():
    # create a database engine and connect to it
    engine = create_engine('sqlite:///DOSM', echo=False)
    conn = engine.connect()

    # create a table to keep track of ingested URLs
    conn.execute('''CREATE TABLE IF NOT EXISTS IngestedURLs
                     (url TEXT PRIMARY KEY, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')

    # get current date
    now = datetime.datetime.now()

    # create list of URLs for all pricecatcher files from current date back to January 2022
    urls = []
    while now.year >= 2022 and now.month >= 1:
        url = f'https://storage.googleapis.com/dosm-public-pricecatcher/pricecatcher_{now.year}-{now.month:02}.parquet'
        urls.append(url)

        # adjust year and month if month is out of range
        if now.month == 1:
            now = now.replace(year=now.year-1, month=12)
        else:
            now = now.replace(month=now.month-1)

    urls.reverse()

    # iterate over URLs and append each file to the Price table in SQLite
    for url in urls:
        # check if URL has already been ingested
        result = conn.execute(f"SELECT url FROM IngestedURLs WHERE url='{url}'").fetchone()
        if result:
            # if URL has already been ingested, skip to the next URL
            print(f"{url} has already been ingested into the Price table.")
            continue

        # read parquet file from URL
        response = requests.get(url)
        content = response.content
        pricecatcher_df = pd.read_parquet(io.BytesIO(content))

        # add 'source' column to dataframe with the URL as its value
        pricecatcher_df['source'] = url

        # Slice the date column into day, month, and year columns
        pricecatcher_df['date'] = pd.to_datetime(pricecatcher_df['date'])
        pricecatcher_df['day'] = pricecatcher_df['date'].dt.day
        pricecatcher_df['month'] = pricecatcher_df['date'].dt.month
        pricecatcher_df['year'] = pricecatcher_df['date'].dt.year

        # append data to the Price table using to_sql() function
        pricecatcher_df.to_sql('Price', conn, if_exists='append', index=False)

        # insert URL into IngestedURLs table
        conn.execute(f"INSERT INTO IngestedURLs (url) VALUES ('{url}')")

        # print message when data ingestion is complete
        print(f"{url} ingested into the Price table.")

        
# create Item table
conn.execute('''CREATE TABLE IF NOT EXISTS Item
             (item_code INTEGER PRIMARY KEY,
             item_category TEXT,
             item TEXT,
             unit TEXT,
             item_group TEXT)''')

# create Premise table
conn.execute('''CREATE TABLE IF NOT EXISTS Premise
             (premise_code INTEGER PRIMARY KEY,
             premise TEXT,
             address TEXT,
             premise_type TEXT,
             state TEXT,
             district TEXT)''')

# create Price table
conn.execute('''CREATE TABLE IF NOT EXISTS Price
             (item_code INTEGER,
             premise_code INTEGER,
             price REAL,
             date DATE,
             source TEXT,
             day  INTEGER,
             month INTEGER,
             year INTEGER,
             FOREIGN KEY (item_code) REFERENCES Item(item_code),
             FOREIGN KEY (premise_code) REFERENCES Premise(premise_code))''')        

# ingest item and premise table
item_df = pd.read_parquet('https://storage.googleapis.com/dosm-public-pricecatcher/lookup_item.parquet')
premise_df = pd.read_parquet('https://storage.googleapis.com/dosm-public-pricecatcher/lookup_premise.parquet')
item_df.to_sql('Item', conn, if_exists='replace', index=False)
premise_df.to_sql('Premise', conn, if_exists='replace', index=False)

#create index to optimise
# Create an index on the 'item_category' column
c.execute('CREATE INDEX IF NOT EXISTS idx_item_category ON Item (item_category)')

# Create an index on the 'year' column
c.execute('CREATE INDEX IF NOT EXISTS idx_year ON Price (year)')

# Create an index on the 'month' column
c.execute('CREATE INDEX IF NOT EXISTS idx_month ON Price (month)')

# Create an index on the 'day' column
c.execute('CREATE INDEX IF NOT EXISTS idx_day ON Price (day)')



try:
    ingest_pricecatcher_data()
except Exception as e:
    error_msg = f"Error occurred during data ingestion: {str(e)}"
    alert(error_msg)


conn.commit()
conn.close()

alert('DAG is done')
        
        
    