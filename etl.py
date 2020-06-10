import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime


def process_song_file(cur, filepath):
    '''
    Extract songs data from data folder
    Insert them into appropiate tables
    
    :param cur: cursor of the database
    :param filepath: the path to which songs data is stored 
    '''
    
    # Open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Extract log data from data folder
    Insert them into appropiate tables
    
    :param cur: cursor of the database
    :param filepath: the path to which songs data is stored 
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    is_NextSong = df['page']=='NextSong'
    is_NextSong_df = df[is_NextSong]

    # convert timestamp column to datetime
    t = is_NextSong_df['ts'].apply(pd.Timestamp, unit='ms')
    records = []
    for el in t:
        records.append([str(el.value), el.hour, el.day, el.weekofyear, el.month, el.year, el.dayofweek])
    
    # insert time data records
    time_data = records
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.rename(columns={'userId':'user_id', 'firstName':'first_name', 'lastName':'last_name'})
    # insert user records
    for i, row in user_df.iterrows():
        if row.user_id != '':
            cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.drop_duplicates().iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
            # insert songplay record
            songplay_data = df[['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent']]
            songplay_data.insert(3, 'song_id', songid)
            songplay_data.insert(4, 'artist_id', artistid)
        
            if songplay_data.values.tolist()[index][1] != '':
                cur.execute(songplay_table_insert, songplay_data.values.tolist()[index])


def process_data(cur, conn, filepath, func):
    '''
    Going through directories looking for .json files to pass
    to param func
    
    :param cur: cursor of the database
    :param conn: connection to database
    :param filepath: path to search for .json files
    :param func: function to be used to process data
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    # Connect to database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()