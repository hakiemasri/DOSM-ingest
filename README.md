# DOSM ETL
https://open.dosm.gov.my/data-catalogue


## Installation

This project uses python. You can install python and its libraries using anaconda (https://docs.anaconda.com/anaconda/install/windows/) or  https://www.python.org/downloads/ 

The library used are:<br/><br/>
pandas: a library for data manipulation and analysis<br/>
sqlalchemy: a library for database interaction that provides a high-level interface for working with SQL databases<br/>
sqlite3: a built-in Python library for working with SQLite databases<br/>
datetime: a built-in Python library for working with dates and times<br/>
requests: a library for making HTTP requests to web servers<br/>
io: a built-in Python library for working with input/output streams<br/>
pyarrow: a library for handling large data sets with efficient memory use and performance<br/>
Flask: A web framework for Python.<br/>
render_template: A function for rendering HTML templates.<br/>
redirect: A function for redirecting to a different URL.<br/>
sqlite3: A module for working with SQLite databases.<br/>
math: A module for mathematical operations.<br/>
SQLAlchemy: An Object Relational Mapper (ORM) for Python.<br/>
text: A module for creating SQL expressions.<br/>
abort: A function for handling HTTP errors.<br/><br/><br/>
 
The databse used is sqlite, this is because my laptop ssd broke and sqlite are easy to install. Download here (https://www.sqlite.org/download.html)
Although, postgress and mysql are also a good option.<br/><br/><br/>
 
For automation and alert, telegram bot api and airflow is used. <br/>
Airflow is installed using docker. https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html<br/>
Telegram api can be referred here.https://telegram-bot-sdk.readme.io/docs/getting-started<br/><br/><br/>
 
Flask is used to create web app.   https://pypi.org/project/Flask/<br/><br/><br/>


## Usage

* First, we ingest the website using ingest.ipynb<br/>
You will need to run the ingest_pricecatcher_data() function, which takes a SQLite database connection as its only argument. The function reads in Parquet files from the DOSM website, and stores the data in a SQLite database that contains three tables: Item, Premise, and Price. The Item table contains information about the items being priced, the Premise table contains information about the premises where the items are being priced, and the Price table contains the actual pricing data.<br/><br/>
  ** Multiple price data are ingested using a  loop and  creates a table called IngestedURLs in the SQLite database to keep track of URLs that have already been ingested. Then it creates a list of URLs for all pricecatcher files from the current date back to January 2022.<br/>
  ** For each URL, the function checks if the URL has already been ingested by querying the IngestedURLs table. If the URL has not been ingested, the function downloads     the Parquet file from the URL, reads it into a Pandas DataFrame, adds a source column to the DataFrame with the URL as its value, and slices the date column into       day, month, and year columns.<br/>
* indexes are created to optimize performance.<br/>
Note that we can also run ipynb files on python terminal if we create copy ipynb codes and ingest.py ,then running it in a python terminal in the same directory.<br/>
```python
python ingest.py
```
    
6.
App.ipynb creates a web application. The application interacts with a DOSM.db to retrieve information about product prices from different premises. The web   application has two endpoints: the first one, accessed by the root URL "/", returns a template called "home.html"<br/><br/>
The second endpoint is accessed when the user submits a form, and it's located at "/result". This endpoint processes the form data, validates it, builds an SQL query, retrieves data from the database, and finally renders a template called "result.html" that displays the results.<br/><br/>
After running the script, we can go to localhost to query our database.<br/><br/>
Comma will be used ',' to query BETWEEN.<br/><br/>
The result will be displayed limited to 50 per page.<br/>

7.
For the automation, please refer airflow/dags/ingest_and_alert.py<br/>
Airflow DAG is used to schedule the (5) process script daily at midnight.<br/>
Refer to Capture.png for screenshot of Airflow.<br/>
Alert function uses telegram sends an alert via the Telegram app to https://t.me/dosmalrtgrp when the ingestion is complete or execption occurs when script is running.




 
   


   
