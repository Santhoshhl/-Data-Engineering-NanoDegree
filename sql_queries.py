import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagingevents;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingsongs "
songplay_table_drop = "DROP TABLE IF EXISTS songplays "
user_table_drop = "DROP TABLE IF EXISTS users "
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time "

# CREATE TABLES

staging_events_table_create= """CREATE TABLE IF NOT EXISTS stagingevents (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts TIMESTAMP,
        userAgent VARCHAR(500),
        userId INTEGER)
    
"""

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stagingsongs(
        num_songs  INTEGER,
        artist_id  VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title  VARCHAR,
        duration FLOAT,
        year INTEGER
    )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1)   PRIMARY KEY,
        start_time  TIMESTAMP   SORTKEY,
        user_id INTEGER,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR)
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users(user_id INTEGER PRIMARY KEY,
        first_name VARCHAR,
        last_name  VARCHAR,
        gender VARCHAR,
        level VARCHAR)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INTEGER ,
        duration FLOAT
    )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP SORTKEY PRIMARY KEY,
        hour INTEGER,
        day  INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday VARCHAR
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stagingevents from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy stagingsongs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId) AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender,
            level
    FROM stagingevents
    WHERE user_id IS NOT NULL
    AND page  ==  'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) AS song_id,title,artist_id,year,duration FROM staging_songs
     WHERE song_id IS NOT NULL""")    
            
 
artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time) AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time)  AS year,
            EXTRACT(dayofweek FROM start_time) AS weekday
    FROM songplays;
""")


songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(a.ts)  AS start_time, 
            a.userId        AS user_id, 
            a.level         AS level, 
            a.song_id       AS song_id, 
            a.artist_id     AS artist_id, 
            a.sessionId     AS session_id, 
            a.location      AS location, 
            a.userAgent     AS user_agent
    FROM staging_events a
    JOIN staging_songs  s   ON (a.song = s.title AND a.artist = s.artist_name)
    AND a.page  ==  'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
