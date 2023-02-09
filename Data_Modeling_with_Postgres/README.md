# Project - One

# **Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.**

The purpose of this database is to help the startup, Sparkify, to improve their recommendation system. The recommendation system should recommend appropriate songs to their users, and reduce the "NextSong" action from the users.

# **How to run the Python scripts**

You can run the Python scripts in the following ways:

# 1.  Create tables:

1. Complete CREATE statements in sql_queries.py for creating tables;
2. Complete DROP statements in sql_queries.py for dropping table if it exists. This is used for resetting the database.
3. Run create_tables.py to create database and tables
4. Run test.ipynb to check if the tables are created correctly. Users may need to modify statements in sql_queries.py if some columns in tables are not correctly created.
- Note: If users want to correct any statements in sql_queries.py, they need to click “Restart kernel” in the jupyter notebooks to close the connection to the database. If there is any other connections to database exist, users cannot re-create the tables for the database.

## 2.  ETL Processes and Pipeline

1. After creating tables, users can start building ETL processes using etl.ipynb file.
2. After each section in the etl.ipynb, users can check the table with test.ipynb; or users can finish all section, then check the tables with test.ipynb.
    - Note: Only one connection to the database can exist at one time. Users need to click “Restart kernel” in etl.ipynb to close the connection to the database if they want to run test.ipynb, and vice versa.
    - Note: If users find problem during testing, they can use create_tables.py to reset tables. Remember to disconnect from database before reset tables.
3. If all tests in test.ipynb have been passed,  users can complete the etl.py based on the etl.ipynb.
    - Note: Before running the ETL pipeline with etl.py, users should reset the database by running create_tables.py.
4. Run test.ipynb to confirm the ETL pipeline.

## 3.  Sanity Tests

1. Complete the sections in test.ipynb. The tests in each sections check the column data types, primary key constraints, not-null constraints, and the on-conflict action during inserting rows.
2. If any warning messages are shown from the test sections, users should modify the statements in sql_queries.py or ELT processes/pipeline to ensure solve the problem.
    - Note: Users need to re-run create_tables.py after they make any changes.

# **An explanation of the files in the repository**

1. create_table.py:
    
    This python file is used for dropping and creating tables. Users need to run this file to reset tables before ETL.
    
2. etl.ipynb
    
    This jupyter notebook provides detailed instructions of the ETL process.  It can read and process a single file from song_data and log_data and loads the data into the table created by the create_tables.py.
    
3. etl.py
    
    This python file provides similar function of ETL.ipynb. It can read and process files from song_data and log_data, and load data into table created by the create_tables.py
    
4. sql_queries.py
    
    This python file contains sql queries that will be imported and used by the etl.py, etl.ipynb, and create_tables.py files.
    
5. test.ipynb
    
    This jupyter notebook is used for checking the database, for example, data type, primary key, etc. It will display the first few rows of the tables created by the users, and check if these tables are correctly created.
    
6. ER_diagram.py
    
    This python file generate the ER-diagram of the database.
    

# **State and justify your database schema design and ETL pipeline.**

## Database schema design:

The database schema is a start schema design. Tables included:

1. Fact Table: songplays. - The songplays table contains records of song plays. Only records with actions of “NextSong” is extracted from the log data.
    
    •  songplay_id (primary key), start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.
    
2. Dimension Tables:
    1. users - user information from the app;
        
        •  user_id (primary key), first_name, last_name, gender, level
        
    2. songs - songs information from the music database;
        
        •  song_id (primary key), title, artist_id, year, duration.
        
    3. artists - artists information from the music database;
        
        •  artist_id, name, location, latitude, longitude
        
    4. **time** - timestamps of records in songplays **table**
        - start_time, hour, day, week, month, year, weekday

3. ER-diagram

    ![sparkifydb_erd.png](sparkifydb_erd.png)
    
## ETL pipeline:

1. Insert rows in songs table and artists tables, with information from the music database;
2. Insert rows in the users table and time table, with information from the log data files;
3. Use the song title and artist name from the log data files to match the song title in the songs table and artist name in the artist table, get the song_id from songs table and artist_id from the artist table for songplays table.

# **Provide example queries and results for song play analysis.**

1. find how many distinct users are in the songplays table:
    
    **Query : %sql SELECT count(distinct(user_id)) FROM songplays**
    
    **Result: Count: 37**
    
2. find how many users are in free or paid level:
    
    **Query : %sql SELECT count(user_id) FROM users group by level**
    
    **Result: Free: 74; Paid 22**
