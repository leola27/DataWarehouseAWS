import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events_table (se_id integer IDENTITY(0,1),
artist text,auth text,first_name text,gender  text,item_in_session integer,last_name text,length decimal,
level text,location text,method text,page text,registration bigint,session_id text,song varchar,status integer,
ts bigint,user_agent text,user_id text);
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs_table (ss_id integer IDENTITY(0,1),
num_songs integer,artist_id text,artist_latitude  text,artist_longitude  text,artist_location text,
artist_name text,song_id text,title text,duration numeric,year integer);
""")

songplay_table_create = ("""CREATE TABLE songplays (songplay_id integer IDENTITY(0,1) NOT NULL, start_time timestamp NOT NULL, user_id varchar(4) NOT NULL, level varchar(10), song_id varchar(50), artist_id varchar(50), session_id varchar(50), location varchar(50), user_agent varchar(200));
""")

user_table_create = ("""CREATE TABLE users (user_id varchar(4) PRIMARY KEY NOT NULL, first_name varchar(50), last_name varchar(50), gender varchar(1), level varchar(10));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id text PRIMARY KEY NOT NULL, title text, artist_id text, year int, duration numeric)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id text PRIMARY KEY, name text, location text, latitude text, longitude text)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY NOT NULL, hour numeric, day numeric, week numeric, month numeric, year numeric, weekday numeric)
""")

# STAGING TABLES

staging_events_copy = ( """COPY staging_events_table
                          FROM {}
                          CREDENTIALS 'aws_iam_role={}'
                          REGION 'us-west-2'
                          JSON AS {}
""".format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'DWH_ROLE_ARN'), config.get('S3', 'LOG_JSONPATH')))

staging_songs_copy = ("""COPY staging_songs_table
                         FROM {}
                         CREDENTIALS 'aws_iam_role={}'
                         REGION 'us-west-2'
                         JSON as 'auto'
                      """.format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'DWH_ROLE_ARN')))

# FINAL TABLES

songplay_table_insert = ("""Insert INTO songplays  ( start_time, user_id , level,song_id,artist_id, session_id, location, user_agent) SELECT DISTINCT DATE_ADD('ms', se.ts, '1970-01-01') AS start_time, se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
FROM staging_events_table se JOIN staging_songs_table ss ON se.song = ss.title AND se.artist = ss.artist_name WHERE se.page = 'NextSong' AND se.user_id IS NOT NULL
""")

user_table_insert = ("""Insert INTO users (user_id, first_name, last_name, gender, level)  SELECT DISTINCT user_id, first_name, last_name, gender, level FROM staging_events_table
""")

song_table_insert = ("""INSERT INTO songs (song_id,title, artist_id, year, duration) SELECT song_id, title, artist_id, year, duration
FROM staging_songs_table
""")

artist_table_insert = ("""INSERT INTO artists (artist_id,  name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs_table
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) SELECT timestamp 'epoch' + ts/1000 * interval '1 second' as 
 ts_insert,DATE_PART(hrs, ts_insert) as hour,DATE_PART(dayofyear, ts_insert) as day, DATE_PART(w, ts_insert) as week, DATE_PART(mons ,ts_insert) as month,DATE_PART(yrs , ts_insert) as year,DATE_PART(dow, ts_insert) as weekday FROM staging_events_table;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
