import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                            artist VARCHAR(MAX) NOT NULL,
                                            auth VARCHAR,
                                            firstName VARCHAR,
                                            gender VARCHAR,
                                            itemInSession INTEGER,
                                            lastName VARCHAR,
                                            length DECIMAL,
                                            level VARCHAR,
                                            location VARCHAR(MAX),
                                            method VARCHAR,
                                            page VARCHAR,
                                            registration TIMESTAMP,
                                            sessionId INTEGER,
                                            song VARCHAR(MAX),
                                            status INTEGER,
                                            ts BIGINT,
                                            userAgent VARCHAR,
                                            userId INTEGER
                                            )
""")


staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                            song_id VARCHAR,
                            artist_id VARCHAR, 
                            artist_latitude double precision, 
                            artist_longitude double precision, 
                            artist_location VARCHAR(MAX),
                            artist_name VARCHAR(MAX) NOT NULL,
                            title VARCHAR(MAX),
                            duration DECIMAL, 
                            year INTEGER);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
                                            songplay_id INT IDENTITY(0,1) PRIMARY KEY,
                                            start_time TIMESTAMP NOT NULL, 
                                            user_id INTEGER NOT NULL, 
                                            level VARCHAR NOT NULL, 
                                            song_id VARCHAR NOT NULL, 
                                            artist_id VARCHAR NOT NULL, 
                                            session_id INTEGER NOT NULL, 
                                            location VARCHAR, 
                                            user_agent VARCHAR)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                user_id int primary key,
                                first_name varchar, 
                                last_name varchar, 
                                gender varchar,
                                level varchar NOT NULL);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR primary key,
                    title VARCHAR(MAX) NOT NULL,
                    artist_id VARCHAR NOT NULL,
                    year int,
                    duration float);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                            artist_id VARCHAR primary key,
                            name VARCHAR(MAX) NOT NULL,
                            location varchar,
                            latitude double precision,
                            longitude double precision);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time TIMESTAMP primary key,
                        hour int,
                        day int,
                        week int,
                        month int, 
                        year int,
                        weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}'
    iam_role '{}'
    format as json '{}'
    TIMEFORMAT 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
    copy staging_songs from '{}'
    iam_role '{}'
    FORMAT AS JSON 'auto'
    TIMEFORMAT 'epochmillisecs';
""").format(SONG_DATA, ARN)


# # FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    )
    
SELECT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
       se.userId as user_id,
       se.level as level,
       ss.song_id as song_id,
       ss.artist_id as artist_id,
       se.sessionId as session_id,
       se.location as location,
       se.userAgent as userAgent
       
FROM staging_events se JOIN staging_songs ss
ON se.artist = ss.artist_name
WHERE se.page = 'NextSong';

""")


user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name, 
        last_name, 
        gender,
        level
        )

    SELECT DISTINCT
           se.userId as user_id,
           se.firstName as first_name,
           se.lastName as last_name,
           se.gender as gender,
           se.level as level

    FROM staging_events se
    WHERE se.page = 'NextSong';

""")


song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
        )

    SELECT DISTINCT
           ss.song_id as song_id,
           ss.title as title,
           ss.artist_id as artist_id,
           ss.year as year,
           ss.duration as duration

    FROM staging_songs ss
    
  """)

                            
artist_table_insert = ("""
    INSERT INTO artists (
            artist_id,
            name,
            location,
            latitude,
            longitude
        )

    SELECT DISTINCT 
           ss.artist_id as artist_id,
           ss.artist_name as name,
           ss.artist_location as location,
           ss.artist_latitude as latitude,
           ss.artist_longitude as longitude

    FROM staging_songs ss
    
  """)

time_table_insert = ("""
    INSERT INTO time (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )

    SELECT DISTINCT
           a.start_time,
           EXTRACT (hour FROM a.start_time) as hour,
           EXTRACT (day FROM a.start_time) as day,
           EXTRACT (week FROM a.start_time) as week,
           EXTRACT (month FROM a.start_time) as month,
           EXTRACT (year FROM a.start_time) as year,
            EXTRACT (weekday FROM a.start_time) as weekday

FROM (SELECT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time 
FROM staging_events se
WHERE se.page = 'NextSong') a;
 """)


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
