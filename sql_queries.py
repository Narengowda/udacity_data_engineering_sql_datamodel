# DROP TABLES

songplay_table_drop = "drop table IF EXISTS songplays "
user_table_drop = "drop table IF EXISTS users"
song_table_drop = "drop table IF EXISTS songs"
artist_table_drop = "drop table IF EXISTS artist"
time_table_drop = "drop table IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays(
   SONGPLAY_ID      SERIAL PRIMARY KEY,
   USER_ID          CHAR(500) NOT NULL,
   START_TIME       CHAR(050) NOT NULL,
   LEVEL            CHAR(500),
   SONG_ID          CHAR(500) NOT NULL,
   ARTIST_ID        CHAR(500) NOT NULL,
   SESSION_ID       CHAR(500),
   LOCATION         CHAR(500),
   USER_AGENT       CHAR(500)
);
""")


user_table_create = ("""
CREATE TABLE users(
   USER_ID           CHAR(500) PRIMARY KEY,
   FIRST_NAME        CHAR(500),
   LAST_NAME         CHAR(500),
   GENDER            CHAR(100),
   LEVEL             CHAR(100)
);
""")

song_table_create = ("""
CREATE TABLE songs(
   SONG_ID          CHAR(500) PRIMARY KEY,
   TITLE            CHAR(500) ,
   ARTIST_ID        CHAR(500),
   YEAR              INTEGER ,
   DURATION          decimal
   );
""")

artist_table_create = ("""
CREATE TABLE artists(
   ARTIST_ID         CHAR(500) PRIMARY KEY,
   NAME              CHAR(500) ,
   LOCATION          CHAR(400),
   LATITUDE          DECIMAL,
   LONGITUDE         DECIMAL
);
""")

time_table_create = ("""
CREATE TABLE TIME(
   START_TIME    TIMESTAMP PRIMARY KEY,
   HOUR          INTEGER,
   DAY           CHAR(500),
   WEEK          INTEGER,
   MONTH         INTEGER,
   YEAR          INTEGER,
   WEEKDAY       BOOLEAN
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays (user_id, level, song_id, artist_id, session_id, location, user_agent) values (%s, %s, %s, %s, %s, %s,%s)
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level) values (%s, %s, %s, %s, %s) ON CONFLICT(user_id)DO NOTHING;
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration) values (%s, %s, %s, %s, %s) ON CONFLICT(song_id)DO NOTHING;
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude) values (%s, %s, %s, %s, %s) ON CONFLICT(artist_id)DO NOTHING;
""")


time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday) values (%s, %s, %s, %s, %s, %s, %s) 
""")

# FIND SONGS
song_select = ("""
select song_id, artists.artist_id from songs join artists on artists.artist_id=songs.artist_id where songs.title=%s and  artists.name=%s and  songs.duration=%s
""")

# QUERY LISTS
create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]