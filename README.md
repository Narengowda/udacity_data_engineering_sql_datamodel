## Songs and artists star schema for Sparkify

Sparkify company wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this schema is to provide Sparkify the ability to easily query data in a timely manner so they can come up with a big picture on user behavior.



To run the project you will need to run following commands
1. python create_tables.py
2. python etl

The following illustrates the data model used in this project:

Data Model
Data Sources

Data resides in two directories that contain files in JSON format:

    data/song_data : Contains metadata about a song and the artist of that song;
    data/log_data : Consists of log files generated by the streaming app based on the songs in the dataset above;


Scripts Usage

    etl.py: Contains the driver script to load data
    create_table.py : contains the code to create the tables
    sql_queries.py : contains all of the sql queries
