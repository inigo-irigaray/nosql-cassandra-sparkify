import csv
import os
import glob
from argparse import ArgumentParser

from cassandra.cluster import Cluster




def create_csv(config):
    print("Creating event data csv file...")
    if config.path is None:
        # Get your current folder and subfolder event data
        filepath = os.getcwd() + '/event_data'
    else:
        filepath = config.path

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = [] 
    for f in file_path_list:
        # reading csv file line by line
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            csvreader = csv.reader(csvfile) 
            next(csvreader) # skips head row
            for line in csvreader:
                full_data_rows_list.append(line) 

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
            
    print("Event data csv file created.\n")
    
    
    

def create_keyspace(config):
    """"""
    session = config.session
    keyspace = config.keyspace
    cluster = Cluster(['127.0.0.1', '9042'])
    print("Connection to session %s..." % session)
    if session == 'local':
        session = cluster.connect()
    else:
        session = cluster.connect(session)
    print("Successfully connected to session %s.\n" % session)
    
    # creates keyspace sparkify
    print("Creating keyspace '%s'..." % keyspace)
    session.execute("DROP KEYSPACE IF EXISTS %s" % keyspace)
    session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = \
                    {'class': 'SimpleStrategy', 'replication_factor':1}" % keyspace)
    print("Keyspace '%s' created.\n" % keyspace)
    
    # connects cluster to sparkify keyspace session
    session.set_keyspace(keyspace)
    
    return cluster, session




def main(config):
    if config.update:
        create_csv(config)
    
    if config.create_keyspace==True:
        cluster, session = create_keyspace(config)
    else:
        cluster = Cluster(['127.0.0.1', '9042'])
        if config.session == 'local':
            session = cluster.connect()
        else:
            session = cluster.connect(session)
            print("Successfully connected to session %s.\n" % session)
            
        # connects cluster to sparkify keyspace session
        session.set_keyspace(config.keyspace)
        
    return cluster, session

            
            
            
if __name__=="__main__":
    parser = ArgumentParser()
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--path', type=str, default=None)
    parser.add_argument('--create_keyspace', action='store_true')
    parser.add_argument('--session', default='local', type=str)
    parser.add_argument('--keyspace', default='sparkify', type=str)
    config = parser.parse_args()
    
    main(config)