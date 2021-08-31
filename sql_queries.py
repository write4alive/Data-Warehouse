import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES - Dropping all tables 

staging_events_table_drop   = "drop table  if exists s_events"
staging_songs_table_drop    = "drop table  if exists s_songs"
user_table_drop             = "drop table  if exists d_users"
song_table_drop             = "drop table  if exists d_songs"
artist_table_drop           = "drop table  if exists d_artists"
time_table_drop             = "drop table  if exists d_time"
songplay_table_drop         = "drop table  if exists f_songplays"

# CREATE TABLES  - Creating Fact and Dim Tables of project.

staging_events_table_create = (""" create table if not exists s_events ( artist varchar,auth varchar,firstname varchar, gender char(1), iteminsession int, lastname varchar, length float, level varchar, location varchar, method varchar, page varchar , registration float, sessionid int, song varchar, status int, ts bigint, useragent varchar, userid int )
""")

staging_songs_table_create  = (""" create table if not exists s_songs ( num_songs int ,artist_id varchar, artist_latitude float, artist_longitude float, artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration float, year int  )
""")

songplay_table_create = (""" create table if not exists f_songplays 
(songplay_id identity(0,1) primary key, start_time timestamp references d_time (start_time), user_id int references d_user (user_id), level varchar, song_id varchar references d_songs(song_id), artist_id varchar references d_artists (artist_id) , session_id int not null , location varchar, user_agent varchar)
""")

user_table_create   =     ("""create table if not exists d_users 
(user_id int primary key, first_name varchar, last_name varchar, gender char(1),level varchar)
""")

song_table_create   =     ("""create table if not exists d_songs 
(song_id varchar primary key, title varchar, artist_id varchar, year int, duration float)
""")

artist_table_create =     ("""create table if not exists d_artists 
(artist_id varchar primary key, name varchar, location varchar, latitude float, longitude float)
""") 

time_table_create   =     ("""create table if not exists d_time 
(start_time timestamp primary key , hour int , day int ,week int ,month int , year int , weekday int )
""")

# Populating staging tables from S3 to Redshift.

staging_events_copy = ("""
                            copy staging_events 
                            from {} credentials  
                            'aws_iam_role={}'   
                            json {}  
                            region 'us-west-2';
                      """).format(
                        config.get("S3","LOG_DATA"), 
                        config.get("IAM_ROLE", "ARN"), 
                        config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
                            copy staging_songs from {} credentials 
                            'aws_iam_role={}\' JSON 'auto' truncatecolumns region 
                            'us-west-2';
                     """).format(
                                config.get("S3","SONG_DATA"), 
                                config.get("IAM_ROLE", "ARN"))



# FINAL TABLES

songplay_table_insert = (""" insert into f_songplays (start_time, user_id, level, song_id,artist_id, session_id, location, user_agent)
                              select 
                              t.start_time as start_time,
                              u.user_id as user_id
                              from s_events se ,s_songs ss
                              where 
                              se.song    = ss.title and
                              se.length  = ss.duration and
                              se.artist_name = ss.artist
                               
                              

""")

user_table_insert     = (""" insert into d_users (user_id, first_name, last_name, gender, level)
                             select
                                   distinct(userid) as user_id,
                                   firstname        as first_name,
                                   lastname         as last_name,
                                   gender,
                                   level
                                   from s_events
                                   where user_id is not null and page = 'NextSong';
""")

song_table_insert     = (""" insert into d_songs( song_id, title, artist_id, year, duration)
                              select 
                                    distinct(song_id),
                                    song_name   as title,
                                    artist_id,
                                    year,
                                    duration
                                    from s_songs
                                    where song_id is not null ;

""")


artist_table_insert   = (""" insert into d_artists (artist_id, name, location, latitude, longitude)
                             select 
                                   distinct(artist_id),
                                   artist_name      as name,
                                   artist_location  as location,
                                   artist_latitude  as latitude,
                                   artist_longitude as longitude
                                   from s_songs
                                   where artist_id is not null;
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT distinct(t.start_time),
                        EXTRACT (HOUR FROM t.start_time), 
                        EXTRACT (DAY FROM t.start_time),
                        EXTRACT (WEEK FROM t.start_time), 
                        EXTRACT (MONTH FROM t.start_time),
                        EXTRACT (YEAR FROM t.start_time), 
                        EXTRACT (WEEKDAY FROM t.start_time) 
                        FROM
                        (SELECT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM s_events where page =' NextSong' ) t;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


# -------------------------------------------------------------------

# Staging table copy command usage:
# config = configparser.ConfigParser()
# config.read('dwh.cfg')
# DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")

# staging_events_copy = ("""copy staging_events from 's3://udacity-dend/log_data'
#                             credentials 'aws_iam_role={}'
#                              compupdate off region 'us-west-2'
#                              timeformat as 'epochmillisecs'
#                              truncatecolumns blanksasnull emptyasnull
#                              json 's3://udacity-dend/log_json_path.json'  ;
# """).format(DWH_ROLE_ARN)

# staging_songs_copy = ("""copy staging_songs from 's3://udacity-dend/song_data/A/A/A'
#                             credentials 'aws_iam_role={}'
#                             format as json 'auto' compupdate off region 'us-west-2';

# -------------------------------------------------------------------


# # Note the equality
# user_table_insert = ("""
# INSERT INTO users (user_id, first_name, last_name, gender, level)
# SELECT  
#   DISTINCT(userId)  AS user_id,
#   firstName     AS first_name,
#   lastName     AS last_name,
#   gender,
#   level
# FROM 
#   staging_events
# WHERE 
#   user_id IS NOT NULL
#   AND page  =  'NextSong';
# """)