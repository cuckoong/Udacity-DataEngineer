# Project - Three

# Summary
## **Using the song data and log data from Sparkify to build a data lake.**

The goal of this project is to use the song data and log data from Sparkify to build the data lake. 
* First step: Load the song data and log data from S3 into Spark;
* Second step: Process the data into analytics tables using Spark, arrange the song data into song table, artist table; 
arrange the log data into user table, time table; and create the songplay table for play records;
* Third step: Save the tables back to S3 in parquet format.

# **Explanation of files in the project**
- dl.cfg: Configuration file for AWS access key and secret key
- etl.py: Python script to load data from S3, process the data and save the tables back to S3
- README.md: Readme file

# **How to run the Python scripts**
Run etl.py via command line:
```python etl.py```
