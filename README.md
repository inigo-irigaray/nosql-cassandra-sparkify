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

<p align=justify>The script creates a single unified file containing all data from the CSV event data files; as well as the non-relational database, if required to do so by the user in the command line.</p>

Command Line Arguments:

        --update                action = 'store_true'                                   help = Update unified event data file.
        --path                  default = None                  type = str              help = Path to metadata.
        --create_keyspace       action = 'store_true'                                   help = Create new keyspace.
        --session               default = 'local'               type = str              help = Name of the Cassandra session to connect.
        --keyspace              default = 'sparkify'            type = str              help = Name of the keyspace to create/open.
        

### ``song_history.py``

<img src=https://github.com/inigo-irigaray/nosql-cassandra-sparkify/blob/master/imgs/song_history.png width=30% height=30% align='right'>

<p align=justify><b>GOAL:</b> Retrieve the artist, song title and song's length in the music app history that was heard during sessionId = 338 and itemInSession = 4</p>

<p align=justify>The script defines in Apache Cassandra the queries to drop and create a song_history table in terms of the goal stated above; extract, transform and load the necessary data from the event data file; and query the relevant information from the table.</p>

<p align=justify>For additional flexibility and functionality of the program, the code includes a number of arguments that allows the user to run it:</p>

<p align=justify>1. As a standalone, creating a keyspace, table, updating and retrieving information for the default sessionId and itemInSession.</p>

<p align=justify>2. Loading an existing keyspace, creating the table, updating and retrieving information for the default sessionId and itemInSession.</p>

<p align=justify>3. Loading an existing keyspace and song_history table, retrieving information for the default or user-determined sessionId and itemInSession.</p>

Command Line Arguments:

        --path                  default = None                  type = str              help = Path to metadata.
        --session               default = 'local'               type = str              help = Name of the Cassandra session to connect.
        --keyspace              default = 'sparkify'            type = str              help = Name of the keyspace to create/open.
        --update                action = 'store_true'                                   help = Update unified event data file.
        --create_keyspace       action = 'store_true'                                   help = Create new keyspace.
        --session_id            default = 338                   type = int              help = Session ID filter.
        --item_in_session       default = 4                     type = int              help = Item in Session filter.

### ``user_history.py`` 

<img src=https://github.com/inigo-irigaray/nosql-cassandra-sparkify/blob/master/imgs/user_history.png width=30% height=30% align='left'>

<p align=justify><b>GOAL:</b> Retrieve every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'</p>

<p align=justify>The script defines in Apache Cassandra the queries to drop and create a user_history table in terms of the goal stated above; extract, transform and load the necessary data from the event data file; and query the relevant information from the table.</p>

<p align=justify>For additional flexibility and functionality of the program, the code includes a number of arguments that allows the user to run it:</p>

<p align=justify>1. As a standalone, creating a keyspace, table, updating and retrieving information for the default song_title.</p>

<p align=justify>2. Loading an existing keyspace, creating the table, updating and retrieving information for the default song_title.</p>

<p align=justify>3. Loading an existing keyspace and user_history table, retrieving information for the default or user-determined song_title.</p>

Command Line Arguments:

        --path                  default = None                  type = str              help = Path to metadata.
        --session               default = 'local'               type = str              help = Name of the Cassandra session to connect.
        --keyspace              default = 'sparkify'            type = str              help = Name of the keyspace to create/open.
        --update                action = 'store_true'                                   help = Update unified event data file.
        --create_keyspace       action = 'store_true'                                   help = Create new keyspace.
        --song_title            default = "'All Hands Against His Own'"                 help = Song title filter. The title of the song must be a substring of the input string.

### ``artist_history.py``

<img src=https://github.com/inigo-irigaray/nosql-cassandra-sparkify/blob/master/imgs/artist_history.png width=30% height=30% align='right'>

<p align=justify><b>GOAL:</b> Retrieve only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182</p>

<p align=justify>The script defines in Apache Cassandra the queries to drop and create a artist_history table in terms of the goal stated above; extract, transform and load the necessary data from the event data file; and query the relevant information from the table.</p>

<p align=justify>For additional flexibility and functionality of the program, the code includes a number of arguments that allows the user to run it:</p>

<p align=justify>1. As a standalone, creating a keyspace, table, updating and retrieving information for the default userId and sessionId.</p>

<p align=justify>2. Loading an existing keyspace, creating the table, updating and retrieving information for the default userId and sessionId.</p>

<p align=justify>3. Loading an existing keyspace and artist_history table, retrieving information for the default or user-determined userId and sessionId.</p>

Command Line Arguments:

        --path                  default = None                  type = str              help = Path to metadata.
        --session               default = 'local'               type = str              help = Name of the Cassandra session to connect.
        --keyspace              default = 'sparkify'            type = str              help = Name of the keyspace to create/open.
        --update                action = 'store_true'                                   help = Update unified event data file.
        --create_keyspace       action = 'store_true'                                   help = Create new keyspace.
        --user_id               default = 10                    type=int                help = User ID filter.
        --session_id            default = 182                   type=int                help = Session ID filter.

## Running the Code

### Option 1.

#### <p align=justify><b>Creating the 'sparkify' keyspace and unified event data file: </b></p>

        $python3 setup.py --create_keyspace --update
      
#### <p align=justify><b>Creating the tables; extracting, transforming, and loading the data into them; and retrieving the default required data for each one:</b></p>

        $python3 song_history.py --update
        $python3 user_history.py --update
        $python3 artist_history.py --update
        
### Option 2.

#### <p align=justify><b>Creating the 'sparkify' keyspace and unified event data file from one of the table files (in this case song_history.py); creating the rest of the tables; extracting, transforming, and loading the data into them; and retrieving the default required data for each one: </b></p>

        $pyhton3 song_history.py --create_keyspace --update
        $python3 user_history.py --update
        $python3 artist_history.py --update
        
### Option 3.

#### <p align=justify><b>Retrieving the user-determined, required data for existing tables: </b></p>

        $pyhton3 song_history.py --session_id=  --item_in_session=
        $python3 user_history.py --song_title=
        $python3 artist_history.py --user_id=  --session_id=
