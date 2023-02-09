import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import TimestampType, StructField, StructType
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
from pyspark.sql.functions import date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a spark session
    :return:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    1. Load song data from S3 bucket with path as input_data;
    2. Create songs table with song_id, title, artist_id, year, duration;
     Drop duplicates of song_id; Change song_id in song table to not null;
     save songs table to parquet files partitioned by year and artist;
    3. Create artists table with artist_id, name, location, latitude,
    longitude;Drop duplicates; Change artist_id in artist table to not null;
    save artists table to parquet files.

    :param spark:  spark session
    :param input_data: S3 bucket, input data path
    :param output_data: S3 bucket, output data path
    :return: None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*', '*.json')

    # read song data files under song_data folder into a dataframe
    df = spark.read.json(song_data)

    # extract columns to create songs table, change song_id nullability to
    # false
    songs_table = df.select(
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration'). dropDuplicates(
        subset=['song_id'])
    newSongSchema = [StructField("song_id", StringType(), False),
                     StructField("title", StringType(), False),
                     StructField("artist_id", StringType(), False),
                     StructField("year", IntegerType(), True),
                     StructField("duration", DoubleType(), True)]
    newSongSchema = StructType(newSongSchema)
    songs_table = spark.createDataFrame(songs_table.rdd, newSongSchema)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id')\
        .parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude') \
        .dropDuplicates(subset=['artist_id'])\
        .withColumnRenamed('artist_name', 'name') \
        .withColumnRenamed('artist_location', 'location') \
        .withColumnRenamed('artist_latitude', 'latitude') \
        .withColumnRenamed('artist_longitude', 'longitude')

    newArtistSchema = [StructField("artist_id", StringType(), False),
                       StructField("name", StringType(), False),
                       StructField("location", StringType(), True),
                       StructField("latitude", DoubleType(), True),
                       StructField("longitude", DoubleType(), True)]
    newArtistSchema = StructType(newArtistSchema)
    artists_table = spark.createDataFrame(artists_table.rdd, newArtistSchema)

    # write artists table to parquet files
    artists_table.write.parquet(
        os.path.join(
            output_data,
            'artists.parquet'),
        'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    1. Load log data from S3 bucket with path as input_data;
    2. Filter by actions ("NextSong") for log_df table;
    3. Create users table with user_id, first_name, last_name, gender, level;
     Drop duplicates; change user_id in user table to integer and not null;
     save users table to parquet files;
    4. Create time table with start_time, hour, day, week, month,
    year, weekday; Drop duplicates; change start_time in time table
    to timestamp and not null; save time table to parquet files;
    5. Create songplays table by joining songs, time and artists tables
    with log_df table; with columns of songplay_id, start_time, user_id,
     level, song_id, artist_id, session_id, location, user_agent, year, month;
     Drop duplicates; Change songplay_id in songplays table to long type
     and not null; save songplays table to parquet files partitioned by
     year and month.

    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*/*/*.json')

    # read log data file
    log_df = spark.read.json(log_data)

    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table, user_id cast to int
    users_table = log_df.select(
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level') .dropDuplicates(
        subset=['userId']) .withColumn(
            'userId',
            col('userId').cast(
                IntegerType())) .withColumnRenamed(
                    'userId',
                    'user_id') .withColumnRenamed(
                        'firstName',
                        'first_name') .withColumnRenamed(
                            'lastName',
        'last_name')

    newUserSchema = [StructField("user_id", IntegerType(), False),
                     StructField("first_name", StringType(), False),
                     StructField("last_name", StringType(), False),
                     StructField("gender", StringType(), True),
                     StructField("level", StringType(), True)]
    newUserSchema = StructType(newUserSchema)
    users_table = spark.createDataFrame(users_table.rdd, newUserSchema)

    # write users table to parquet files
    users_table.write.parquet(
        os.path.join(
            output_data,
            'users.parquet'),
        'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x) // 1000))
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda x: datetime.fromtimestamp(
            int(x)), TimestampType())
    log_df = log_df.withColumn('datetime', get_datetime(log_df.timestamp))

    # extract columns to create time table, drop row with null value in
    # datetime column, rename datetime to start_time
    time_table = log_df.select('datetime').dropDuplicates() \
        .withColumnRenamed('datetime', 'start_time')

    time_table = time_table.withColumn('hour', hour('start_time')) \
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', date_format('start_time', 'E'))

    newTimeSchema = [StructField("start_time", TimestampType(), False),
                     StructField("hour", IntegerType(), False),
                     StructField("day", IntegerType(), False),
                     StructField("week", IntegerType(), False),
                     StructField("month", IntegerType(), False),
                     StructField("year", IntegerType(), False),
                     StructField("weekday", StringType(), False)]

    newTimeSchema = StructType(newTimeSchema)
    time_table = spark.createDataFrame(time_table.rdd, newTimeSchema)

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(
        'year',
        'month').parquet(
        os.path.join(
            output_data,
            'time.parquet'),
        'overwrite')

    # read in song data from parquet files songs.parquet
    songs_table = spark.read.parquet(
        os.path.join(output_data, 'songs.parquet'))

    # extract columns from joined song and log datasets to create songplays
    # table, add songplay_id
    log_df.createOrReplaceTempView("log_data")
    songs_table.createOrReplaceTempView("song_table")
    time_table.createOrReplaceTempView("time_table")

    songplays_table = spark.sql("""
        SELECT monotonically_increasing_id() as songplay_id,
            t.start_time as start_time,
            l.userId as user_id,
            l.level,
            s.song_id,
            s.artist_id,
            l.sessionId as session_id,
            l.location,
            l.userAgent as user_agent,
            t.year,
            t.month
        FROM log_data l
        JOIN song_table s ON l.song = s.title AND l.length = s.duration
        JOIN time_table t ON l.datetime = t.start_time
        """)

    songplays_table = songplays_table.withColumn(
        'user_id', col('user_id').cast(IntegerType()))

    newSongplaysSchema = [
        StructField("songplay_id", LongType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("level", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("session_id", LongType(), True),
        StructField("location", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False)]

    newSongplaysSchema = StructType(newSongplaysSchema)
    songplays_table = spark.createDataFrame(
        songplays_table.rdd, newSongplaysSchema)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(
        'year',
        'month').parquet(
        os.path.join(
            output_data,
            'songplays.parquet'),
        'overwrite')


def main():
    """
    Main function to run the ETL process
    :return: None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacitydatalakepanda/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()