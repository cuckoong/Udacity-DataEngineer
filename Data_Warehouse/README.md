# Project - Two

# **Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.**

The goal of this database is to assist the firm Sparkify in determining what songs their users would enjoy, how to enhance their user's song suggestion system, and how to persuade their customers to upgrade to a paid subscription.

# **State and justify your database schema design and ETL pipeline.**

## Database schema design:

The database schema is a star schema design. Tables included:

1. Fact Table: songplays. - The songplays table contains records of song plays. Only records with actions of “NextSong” is extracted from the log data.
    
    •  songplay_id (primary key), start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.
    
2. Dimension Tables:
    1. users - user information from the app;
        
        •  user_id (primary key), first_name, last_name, gender, level
        
    2. songs - songs information from the music database;
        
        •  song_id (primary key), title, artist_id, year, duration.
        
    3. artists - artists information from the music database;
        
        •  artist_id, name, location, latitude, longitude
        
    4. time - timestamps of records in songplays table
        - start_time, hour, day, week, month, year, weekday

## ETL pipeline:

1. copy data from songs data and logs data into staging events and staging songs table;
2. Join corresponding columns from staging events and staging song, select and insert rows in the users, song, artist, and time tables.

# **How to run the Python scripts**

1. Run create_tables.py to create database and tables
2. After creating tables, users can start building ETL processes using etl.py file.
