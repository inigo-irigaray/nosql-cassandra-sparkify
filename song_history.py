import csv
import glob
from argparse import ArgumentParser

import setup




# SONG HISTORY TABLE

query = "SELECT artist, song_title, song_length \
        FROM song_history \
        WHERE session_id=%s and item_in_session=%s"

drop_song_history = "DROP TABLE IF EXISTS song_history"

create_song_history_table = ("CREATE TABLE IF NOT EXISTS song_history (session_id int,\
                                                                       item_in_session int,\
                                                                       song_title text,\
                                                                       artist text,\
                                                                       song_length float,\
                                                                       PRIMARY KEY (session_id, item_in_session)\
                                                                       )"
                            )

song_history_insert = "INSERT INTO song_history (session_id, item_in_session, song_title, artist, song_length) \
                      VALUES (%s, %s, %s, %s, %s)"
    



def create_table(session, drop_query, create_query, update):
    if update==True:
        session.execute(drop_query)
        print("Creating song_history table...")
        session.execute(create_query)
        print("Table for song_history created.\n")
    else:
        pass




def update_song_history(session, query):
    file = 'event_datafile_new.csv'
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.DictReader(f)
        for line in csvreader:
            session.execute(query, (int(line['sessionId']), int(line['itemInSession']), line['song'],
                                    line['artist'], float(line['length'])))
            
            
            

def process_song_history(session, query, session_id, item_in_session):
    assert isinstance(session_id, int)
    assert isinstance(item_in_session, int)
    
    rows = session.execute(query % (session_id, item_in_session))
    
    print("\nSong history for Session ID: %s; Items in Session: %s.\n" % (session_id, item_in_session))
    for row in rows:
        print("Artist:",row.artist, "\nSong:", row.song_title, "\nLength:", row.song_length, "\n\n")
        
        
        
        
def main(config, session):
    # create song_history table
    create_table(session,
                 drop_song_history,
                 create_song_history_table,
                 config.update)
    
    # update event data if required
    if config.update:
        print('Updating event data...')
        update_song_history(session, song_history_insert)
        print('Event data up to date.')
    
    # query song history
    process_song_history(session=session,
                         query=query,
                         session_id=config.session_id,
                         item_in_session=config.item_in_session)

    
    
    
if __name__=="__main__":
    parser = ArgumentParser()
    parser.add_argument('--path', type=str, default=None)
    parser.add_argument('--session', default='local', type=str)
    parser.add_argument('--keyspace', default='sparkify', type=str)
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--create_keyspace', action='store_true')
    parser.add_argument('--session_id', default=338, type=int)
    parser.add_argument('--item_in_session', default=4, type=int)
    
    config = parser.parse_args()
    cluster, session = setup.main(config)
    main(config, session)
    
    cluster.shutdown()
    session.shutdown()