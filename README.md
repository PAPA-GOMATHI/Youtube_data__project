# Youtube_data__project
PROJECT TITLE - 
---------------
Youtube Data harvesting and data warehousing using MongoDB, Postgresql, Streamlit

INTRODUCTION -
--------------
1. Creating a Streamlit application that allows users to access and analyze the data from multiple Youtube channels through the Youtube 
   api key with all youtube channels ID's
2. The app should facilitate storing the data in a MongoDB database and allow users to collect data from up to 10 different channels like    Channel details, Playlists details, Video details, Comments details
3. It should offer the capability to migrate selected channel data from the data lake(MongoDB) to a SQL database for further analysis

KEY SKILLS FROM THE PROJECT - 
-----------------------------
1. Python programming
2. Data collection and API integration from the Youtube
3. Data Management using MongoDB(Compass) and SQL (PostgreSQL)

TECHNOLOGY STACK USED -
-----------------------
1. PYTHON
2. SQL
3. STREAMLIT
4. MongoDB
5. Google API client library

INSTALLATION STAGE -
---------------------
*libraries need to be installed on the computer before starting this project are:

1. pip install google-api-python-client
2. pip install pymongo
3. pip install pandas
4. pip install psycopg2
5. pip install streamlit

FEATURES - 
-----------
1. Retrieve data from the YouTube API, including channel information, playlists, videos, and comments
2. Store the retrieved data in a MongoDB database
3. Migrate the data to a SQL data warehouse
4. Perform queries on the SQL data warehouse

PROJECT APPROACH -
------------------
1. Starting the establishment of the connection with the Youtube API v3 key
2. Store the retrieved data in a MongoDB data lake
3. Transferring the collected data from multiple channels namely the channels,videos, playlists and comments to a SQL data warehouse
4. Utilize SQL queries to join tables within the SQL data warehouse and retrieve specific channel data based on user input
5. The retrieved data is displayed within the Streamlit application

Thank you!
