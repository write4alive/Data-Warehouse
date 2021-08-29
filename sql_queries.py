import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = ""
staging_songs_table_drop = ""
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create= ("""
""")

staging_songs_table_create = ("""
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]





# -------------------------------------------------------------------
# DROP TABLES - First we drop all fact and dim tables if they exists.
user_table_drop = "drop table if exists  d_users"
song_table_drop = "drop table  if exists d_songs"
artist_table_drop = "drop table  if exists d_artists"
time_table_drop = "drop table  if exists d_time"
songplay_table_drop = "drop table if exists f_songplays"

# CREATE TABLES  - Creating Fact and Dim Tables of project.

songplay_table_create = ("""create table if not exists f_songplays 
(songplay_id serial primary key, start_time bigint, user_id int, level varchar, song_id varchar, artist_id varchar , session_id int not null , location varchar, user_agent varchar)
""")

user_table_create =     ("""create table if not exists d_users 
(user_id int primary key, first_name varchar, last_name varchar, gender char(1),level varchar)
""")

song_table_create =     ("""create table if not exists d_songs 
(song_id varchar primary key, title varchar, artist_id varchar, year int, duration float)
""")

artist_table_create =   ("""create table if not exists d_artists 
(artist_id varchar primary key, name varchar, location varchar, latitude float, longitude float)
""") 

time_table_create =     ("""create table if not exists d_time 
(start_time bigint primary key , hour int , day int ,week int ,month int , year int , weekday int )
""")

# INSERT RECORDS - Insert queries of fact and dim tables. Aoiding data duplication with using " ON CONFLICT DO ".

songplay_table_insert = ("""insert into f_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values(%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""insert into d_users(user_id, first_name, last_name, gender, level)
values(%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""insert into d_songs(song_id, title, artist_id, year, duration)
values(%s,%s,%s,%s,%s) ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""insert into d_artists(artist_id, name, location, latitude, longitude)
values(%s,%s,%s,%s,%s) ON CONFLICT (artist_id) DO NOTHING
""")

time_table_insert = ("""insert into d_time(start_time, hour, day, week, month, year, weekday)
values(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (start_time) DO NOTHING;
""")
# -------------------------------------------------------------------