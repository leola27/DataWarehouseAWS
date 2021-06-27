# Udacity Data Engineer Nanodegree project

## Data Warehouse in AWS

### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### ETL process and Database schema design
ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables

The database schema design is snowflake. It is benefitial considering the goals of analytics team since it organises the data in the fact table, showing how and when users play songs, and several dimension tables, providing details on a particular dimension of the data (song,user, artist,time).
#### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong:songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
#### Dimension Tables
##### users
users in the app: user_id, first_name, last_name, gender, level
##### songs
songs in music database: song_id, title, artist_id, year, duration
##### artists
artists in music database: artist_id, name, location, latitude, longitude
##### time 
timestamps of records in songplays broken down into specific units: start_time, hour, day, week, month, year, weekday

### Files in repository
#### create_table.py 
Contains fact and dimension tables for the star schema in Redshift.
#### etl.py
contains data from S3 into staging tables on Redshift and then processes that data into analytics tables on Redshift.
#### sql_queries.py
contains SQL statements, which will be imported into the two other files above.
