import csv
import glob
from argparse import ArgumentParser

import setup




# USER HISTORY TABLE

query = "SELECT first_name, last_name \
        FROM user_history \
        WHERE song_title=%s"

drop_user_history = "DROP TABLE IF EXISTS user_history"

create_user_history_table = ("CREATE TABLE IF NOT EXISTS user_history (song_title text,\
                                                                       user_id int,\
                                                                       first_name text,\
                                                                       last_name text,\
                                                                       PRIMARY KEY (song_title, first_name, last_name)\
                                                                       )"
                            )

user_history_insert = "INSERT INTO user_history (song_title, user_id, first_name, last_name) \
                      VALUES (%s, %s, %s, %s)"
    



def create_table(session, drop_query, create_query, update):
    if update==True:
        session.execute(drop_query)
        print("Creating user_history table...")
        session.execute(create_query)
        print("Table for user_history created.\n")
    else:
        pass




def update_user_history(session, query):
    file = 'event_datafile_new.csv'
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.DictReader(f)
        for line in csvreader:
            session.execute(query, (line['song'], int(line['userId']), line['firstName'], line['lastName'])) 
            
            
            

def process_user_history(session, query, song_title):
    assert isinstance(song_title, str)
    
    rows = session.execute(query % (song_title))
    
    print("\nUser history for Song Title: %s.\n" % (song_title))
    for row in rows:
        #print(row)
        print("User Name: ", row.first_name, row.last_name, "\n\n")
        
        
        
        
def main(config, session):
    # create user_history table
    create_table(session,
                 drop_user_history,
                 create_user_history_table,
                 config.update)
    
    # update event data if required
    if config.update:
        print('Updating event data...')
        update_user_history(session, user_history_insert)
        print('Event data up to date.')
    
    # query user history
    process_user_history(session=session,
                         query=query,
                         song_title=config.song_title)

    
    
    
if __name__=="__main__":
    parser = ArgumentParser()
    parser.add_argument('--path', type=str, default=None)
    parser.add_argument('--session', default='local', type=str)
    parser.add_argument('--keyspace', default='sparkify', type=str)
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--create_keyspace', action='store_true')
    parser.add_argument('--song_title', default="'All Hands Against His Own'", type=str,
                        help='The title of the song must be a substring of the input string.')
    
    config = parser.parse_args()
    cluster, session = setup.main(config)
    main(config, session)
    
    cluster.shutdown()
    session.shutdown()