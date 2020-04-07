import csv
import glob
from argparse import ArgumentParser

import setup




# ARTIST HISTORY TABLE

query = "SELECT artist, song_title, first_name, last_name, item_in_session \
        FROM artist_history \
        WHERE user_id=%s and session_id=%s"

drop_artist_history = "DROP TABLE IF EXISTS artist_history"

create_artist_history_table = ("CREATE TABLE IF NOT EXISTS artist_history (user_id int,\
                                                                           session_id int,\
                                                                           item_in_session int, \
                                                                           artist text,\
                                                                           song_title text,\
                                                                           first_name text,\
                                                                           last_name text,\
                                                                           PRIMARY KEY (user_id, session_id, item_in_session)\
                                                                           )"
                              )

artist_history_insert = "INSERT INTO artist_history \
                        (user_id, session_id, item_in_session, artist, song_title, first_name, last_name) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s)"
    



def create_table(session, drop_query, create_query, update):
    if update==True:
        session.execute(drop_query)
        print("Creating artist_history table...")
        session.execute(create_query)
        print("Table for artist_history created.\n")
    else:
        pass




def update_artist_history(session, query):
    file = 'event_datafile_new.csv'
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.DictReader(f)
        for line in csvreader:
            session.execute(query, (int(line['userId']), int(line['sessionId']), int(line['itemInSession']), line['artist'],
                                    line['song'], line['firstName'], line['lastName']))  
            
            
            

def process_artist_history(session, query, user_id, session_id):
    assert isinstance(user_id, int)
    assert isinstance(session_id, int)
    
    rows = session.execute(query % (user_id, session_id))
    
    print("\nArtist history for User ID: %s; Session ID: %s.\n" % (user_id, session_id))
    for row in rows:
        print("Artist:",row.artist, "\nSong:", row.song_title, "\nUser Name:", row.first_name, row.last_name,
              "\nItem In Session:", row.item_in_session, "\n\n")
        
        
        
        
def main(config, session):
    # create artist_history table
    create_table(session,
                 drop_artist_history,
                 create_artist_history_table,
                 config.update)
    
    # update event data if required
    if config.update:
        print('Updating event data...')
        update_artist_history(session, artist_history_insert)
        print('Event data up to date.')
    
    # query artist history
    process_artist_history(session=session,
                           query=query,
                           user_id=config.user_id,
                           session_id=config.session_id)

    
    
    
if __name__=="__main__":
    parser = ArgumentParser()
    parser.add_argument('--path', type=str, default=None)
    parser.add_argument('--session', default='local', type=str)
    parser.add_argument('--keyspace', default='sparkify', type=str)
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--create_keyspace', action='store_true')
    parser.add_argument('--user_id', default=10, type=int)
    parser.add_argument('--session_id', default=182, type=int)
    
    config = parser.parse_args()
    cluster, session = setup.main(config)
    main(config, session)
    
    cluster.shutdown()
    session.shutdown()