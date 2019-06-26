import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop =       "DROP TABLE IF EXISTS songplays;"
user_table_drop =           "DROP TABLE IF EXISTS users;"
song_table_drop =           "DROP TABLE IF EXISTS songs;"
artist_table_drop =         "DROP TABLE IF EXISTS artists;"
time_table_drop =           "DROP TABLE IF EXISTS time;"

# CREATE TABLES

# Staging Tables
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (  
                                 artist VARCHAR,
                                 auth VARCHAR,
                                 firstname VARCHAR,
                                 gender VARCHAR,
                                 item_in_session INTEGER,
                                 lastname VARCHAR,
                                 length REAL,
                                 level VARCHAR,
                                 location VARCHAR,
                                 method VARCHAR,
                                 page VARCHAR,
                                 registration REAL,
                                 session_id INTEGER, 
                                 song VARCHAR,
                                 status INTEGER,
                                 time_stamp VARCHAR,
                                 user_agent VARCHAR,
                                 user_id INTEGER);
                              """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS  staging_songs(
                                 num_songs INTEGER,
                                 artist_id TEXT,
                                 artist_latitude REAL,
                                 artist_longitude REAL,
                                 artist_location TEXT,
                                 artist_name TEXT,
                                 song_id TEXT,
                                 title TEXT,
                                 duration REAL,
                                 year TEXT);
                              """)

# Dimension Tables
user_table_create =          ("""CREATE TABLE IF NOT EXISTS users(
                                 user_id INTEGER PRIMARY KEY, 
                                 first_name VARCHAR,
                                 last_name VARCHAR, 
                                 gender VARCHAR , 
                                 level VARCHAR);
                             """)

artist_table_create =        ("""CREATE TABLE IF NOT EXISTS artists(
                                 artist_id VARCHAR PRIMARY KEY, 
                                 name VARCHAR, 
                                 location VARCHAR, 
                                 latitude FLOAT, 
                                 longitude FLOAT);
                             """)

song_table_create =          ("""CREATE TABLE IF NOT EXISTS songs(
                                 song_id VARCHAR PRIMARY KEY, 
                                 title VARCHAR,
                                 artist_id VARCHAR NOT NULL, 
                                 year TEXT, 
                                 duration FLOAT);
                             """)

time_table_create =          ("""CREATE TABLE IF NOT EXISTS time(
                                 start_time TIMESTAMP PRIMARY KEY, 
                                 hour INTEGER NOT NULL, 
                                 day INTEGER NOT NULL,
                                 week INTEGER NOT NULL, 
                                 month INTEGER NOT NULL, 
                                 year INTEGER NOT NULL, 
                                 weekday VARCHAR NOT NULL);
                             """)
# FACT TABLES
songplay_table_create =      ("""CREATE TABLE IF NOT EXISTS songplays(
                                 songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
                                 start_time TIMESTAMP REFERENCES time(start_time), 
                                 user_id INTEGER REFERENCES users(user_id),
                                 level VARCHAR, 
                                 song_id VARCHAR REFERENCES songs(song_id), 
                                 artist_id VARCHAR REFERENCES artists(artist_id), 
                                 session_id VARCHAR,
                                 location VARCHAR, 
                                 user_agent VARCHAR);
                             """)
# STAGING TABLES

staging_events_copy =    (""" COPY staging_events 
                              FROM 's3://udacity-dend/log-data'
                              credentials 'aws_iam_role={}'
                              compupdate off
                              region 'us-west-2'  
                              JSON 's3://udacity-dend/log_json_path.json';
                          """).format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy =     (""" copy staging_songs from 's3://udacity-dend/song-data'
                              credentials 'aws_iam_role={}'
                              compupdate off 
                              region 'us-west-2'
                              JSON 'auto' truncatecolumns;
                         """).format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

user_table_insert =     ("""INSERT INTO users (user_id , first_name , last_name , gender , level) 
                            SELECT user_id,
                            firstname,
                            lastname,
                            gender,
                            level
                            FROM {} 
                            WHERE user_id IS NOT NULL
                            AND page='NextSong';
                        """).format('staging_events')


song_table_insert =     ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                            SELECT 
                            distinct(song_id),
                            title,
                            artist_id,
                            year,
                            duration
                            FROM {}
                            WHERE song_id IN (SELECT distinct(song_id) FROM staging_songs);
                         """).format('staging_songs')

artist_table_insert =   ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                            SELECT 
                            distinct(artist_id),
                            artist_name,
                            artist_location,
                            artist_latitude,
                            artist_longitude
                            FROM {};
                        """).format('staging_songs')

time_table_insert =     ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                            SELECT DISTINCT timestamp 'epoch'+cast(time_stamp as bigint)/1000*interval '1 second' AS start_time,
                            EXTRACT(hours from start_time) AS hour,
                            EXTRACT(day from start_time) AS day,
                            EXTRACT(week FROM start_time) AS week,
                            EXTRACT(month FROM start_time) AS month,
                            EXTRACT(year FROM start_time) AS year,
                            EXTRACT(weekday FROM start_time) AS weekday
                            FROM {}
                            WHERE page='NextSong';
                        """).format('staging_events')

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level , song_id, 
                            artist_id, session_id, location, user_agent) 
                            SELECT timestamp 'epoch'+cast(se.time_stamp as bigint)/1000*interval '1 second' AS start_time,
                            se.user_id,
                            se.level,
                            s.song_id,
                            a.artist_id,
                            se.session_id,
                            se.location,
                            se.user_agent
                            FROM songs  s
                            LEFT OUTER JOIN artists a ON a.artist_id=s.artist_id
                            LEFT OUTER JOIN staging_events se ON (s.title=se.song
                            AND a.name=se.artist
                            AND s.duration=se.length)
                            WHERE se.page='NextSong';
                         """)


# SELECT QUERIES

staging_events_select=   ("""SELECT COUNT(*) FROM staging_events""")
staging_songs_select=    ("""SELECT COUNT(*) FROM staging_songs""")
user_table_select=       ("""SELECT COUNT(*) FROM users""")
song_table_select=       ("""SELECT COUNT(*) FROM songs""")
artist_table_select=     ("""SELECT distinct COUNT(*) FROM artists""")
time_table_select=       ("""SELECT COUNT(*) FROM time""") 
songplay_table_select=   ("""SELECT COUNT(*) FROM songplays""") 

# QUERY LISTS

create_table_queries = [user_table_create,artist_table_create, song_table_create , time_table_create,songplay_table_create]   #staging_events_table_create, staging_songs_table_create 
drop_table_queries =   [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries =   [staging_events_copy,staging_songs_copy]  #staging_events_table_drop, staging_songs_table_drop, 
select_table_queries=  [staging_events_select,staging_songs_select,user_table_select,song_table_select,artist_table_select,
                        time_table_select,songplay_table_select]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert] 
