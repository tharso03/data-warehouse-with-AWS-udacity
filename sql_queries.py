import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config['S3']['LOG_DATA']
DWS_ROLE_ARN = config['IAM_ROLE']['ARN']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_tables"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

## Create staging table: staging_events

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR(50),
    gender VARCHAR(1),
    item_in_session INTEGER,
    last_name VARCHAR(50),
    length NUMERIC,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration NUMERIC,
    session_id INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    user_agent VARCHAR,
    user_id INTEGER
)
""")

## Create staging table: staging_songs

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration NUMERIC,
    year INTEGER
)
""")

## Create fact table: songplays

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time BIGINT NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INTEGER NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
)
""")

## Create dimention table: users

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id INTEGER PRIMARY KEY,
first_name VARCHAR(50),
last_name VARCHAR(50),
gender VARCHAR(1),
level VARCHAR NOT NULL
)
""")

## Create dimention table: songs

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INTEGER,  
    duration NUMERIC
)
""")

## Create dimention table: artists

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR, 
    latitude NUMERIC,
    longitude NUMERIC
)
""")

## Create dimention table: time

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time BIGINT PRIMARY KEY,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday BOOLEAN NOT NULL
)
""")

# STAGING TABLES

# Fill staging_events with data from S3.

staging_events_copy = ("""COPY staging_events FROM {}
IAM_ROLE {}
JSON {}
region 'us-west-2'
""").format(LOG_DATA, DWS_ROLE_ARN, LOG_JSONPATH)

# Fill staging_songs with data from S3.

staging_songs_copy = ("""COPY staging_songs FROM {}
IAM_ROLE {}
FORMAT AS JSON 'auto'
region 'us-west-2'
""").format(SONG_DATA, DWS_ROLE_ARN)

# FINAL TABLES

# Insert data into songplay

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id , artist_id , session_id , location, user_agent)
SELECT DISTINCT st_ev.ts AS start_time, st_ev.user_id, st_ev.level, st_sg.song_id, st_sg.artist_id, st_ev.session_id, st_ev.location, st_ev.user_agent
FROM staging_events st_ev
JOIN staging_songs st_sg ON st_ev.artist = st_sg.artist_name AND st_ev.song = st_sg.title
WHERE st_ev.page = 'NextSong'
""")

# Insert data into users

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE page = 'NextSong'
AND user_id IS NOT NULL
""")

# Insert data into songs

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

# Insert data into artists

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

# Insert data into time

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
ts AS start_time,
EXTRACT(hour FROM ts) AS hour,
EXTRACT(day FROM ts) AS day,
EXTRACT(week FROM ts) AS week,
EXTRACT(month FROM ts) AS month,
EXTRACT(year FROM ts) AS year,
EXTRACT(dayofweek FROM ts) AS weekday
FROM staging_events
WHERE page = 'NextSong'
""")

# QUERY LISTS

# Join tables according to their actions: creating, dropping, copying and inserting.

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]