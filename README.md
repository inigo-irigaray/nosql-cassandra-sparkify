# Sparkify Music App Song & User Activity Data Modeling with Cassandra (NoSQL)

## Challenge

<p align=justify>A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.</p>

<p align=justify>They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.</p>

## Project Description

### Data

The first dataset is a directory of data files in CSV format called ``event_data``. The data is partitioned in files by date in the following way: ``event_data/yyyy-mm-dd.csv files``. Here is a snapshot of the data:

<img src=https://github.com/inigo-irigaray/nosql-cassandra-sparkify/blob/master/imgs/image_event_datafile_new.jpg>

## Code Description

### ``setup.py``

<p align=justify>The script specifies the SQL commands to DROP and CREATE the songplays, users, songs, artists and time tables in the relational database, as well as the INSERT command to fill in the data and a specific SELECT utility command to help in the processing of data for the songplays table.</p>

### ``song_history.py``
<img src=https://github.com/inigo-irigaray/nosql-cassandra-sparkify/blob/master/imgs/song_history.png width=30% height=30% align='right'>
<p align=justify><b>GOAL:</b> Retrieve the artist, song title and song's length in the music app history that was heard during sessionId = 338 and itemInSession = 4</p>

<p align=justify>The script defines the functions to create the Sparkify relational database, create and drop the five tables aforementioned from the imported SQL commands in the previous script. It can be run as standalone to create empty tables or as part of the ETL pipeline in the following script.</p>

### ``user_history.py`` 
<img src=https://github.com/inigo-irigaray/nosql-cassandra-sparkify/blob/master/imgs/user_history.png width=30% height=30% align='left'>
<p align=justify><b>GOAL:</b> Retrieve only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182</p>
<p align=justify>The script extracts, transforms and loads the data into the required tables in the Sparkify database from the song and log files in JSON format. It can additionally create the required tables and dataset if predetermined by the user with the --create_db argument in the command line.</p>

### ``artist_history.py``
<img src=https://github.com/inigo-irigaray/nosql-cassandra-sparkify/blob/master/imgs/artist_history.png width=30% height=30% align='right'>
<p align=justify><b>GOAL:</b> Retrieve every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'</p>
<p align=justify>The script extracts, transforms and loads the data into the required tables in the Sparkify database from the song and log files in JSON format. It can additionally create the required tables and dataset if predetermined by the user with the --create_db argument in the command line.</p>

## Running the Code

### Option 1. Creating Databases and Tables from Scratch:

#### <p align=justify><b>1.A. Creating the Sparkify database and tables required from ``create_tables.py``first, and then extracting, transforming and loading the data from ``etl.py``: </b></p>

        $python3 create_tables.py
        $python3 etl.py
      
#### 1.B. Performing all actions directly from ``etl.py``:

        $python3 etl.py --create_db
        
### Option 2. Extracting, transforming and loading the data into the required existing tables and database:

        $pyhton3 etl.py
