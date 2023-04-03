# DOSM ETL
This is an ETL that ingest data from https://open.dosm.gov.my/data-catalogue PriceCatcher section and creates a web app that is able to query the ingested data.
<br/>Automation and alert are also included.


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
  * Multiple price data are ingested using a  loop and  creates a table called IngestedURLs in the SQLite database to keep track of URLs that have already been ingested. Then it creates a list of URLs for all pricecatcher files from the current date back to January 2022.<br/>
  * For each URL, the function checks if the URL has already been ingested by querying the IngestedURLs table. If the URL has not been ingested, the function downloads     the Parquet file from the URL, reads it into a Pandas DataFrame, adds a source column to the DataFrame with the URL as its value, and slices the date column into       day, month, and year columns.<br/>
* Premise and item data are also ingested the same way as price data but only once.
* indexes are created to optimize performance.<br/><br/>
Note that we can also run ipynb files on python terminal if we create copy ipynb codes and ingest.py ,then running it in a python terminal in the same directory.<br/>
```python
python ingest.py
```
<br/><br/>  
* Secondly, run App.ipynb which will create a web application. The application interacts with DOSM.db to retrieve information."<br/>
* Open a web browser and go to http://localhost:5000/.
* Select the desired item category, month, year, and day from the search form. Comma will be used ',' to query BETWEEN
* The result will be displayed limited to 50 per page to save query time.<br/><br/>


* For the automation, please refer airflow/dags/ingest_and_alert.py<br/>
* Airflow DAG is used to schedule the process script daily at midnight.<br/> 
  * Run ingestion process 
  *  uses telegram sends an alert via the Telegram app to https://t.me/dosmalrtgrp when the ingestion is complete or execption occurs when script is running.
<br/> 
Refer to Capture.png for screenshot of Airflow.<br/>




 
   


   
