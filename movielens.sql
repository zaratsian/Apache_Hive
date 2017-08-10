

#######################################################################################################################
#
#   Download Movie Dataset from here: https://grouplens.org/datasets/movielens/
#   Here's the direct link: http://files.grouplens.org/datasets/movielens/ml-latest.zip
#
#   Additional info and background can be found here as well: https://www.kaggle.com/grouplens/movielens-20m-dataset
#
#   Contents (26 million reviews):
#       1.)  movie.csv that contains movie information
#       2.)  rating.csv that contains ratings of movies by users
#       3.)  link.csv that contains identifiers that can be used to link to other sources
#       4.)  tag.csv that contains tags applied to movies by users
#       5.)  genome_scores.csv that contains movie-tag relevance data
#       6.)  genome_tags.csv that contains tag descriptions
#
#######################################################################################################################


#######################################################################################################################
Step 0: If you have not already done so, download the data
        wget http://files.grouplens.org/datasets/movielens/ml-latest.zip -o /tmp/ml-latest.zip
#######################################################################################################################

#######################################################################################################################
Step 1: Copy the movielens-20m-dataset.zip to a node within your cluster. 
        Recommended location: /tmp/ml-latest.zip
#######################################################################################################################

#######################################################################################################################
Step 2: Unzip the data
#######################################################################################################################

#######################################################################################################################
Step 3: Create HDFS directories:
#######################################################################################################################

hadoop fs -mkdir /tmp/ml-latest
hadoop fs -mkdir /tmp/ml-latest/movies
hadoop fs -mkdir /tmp/ml-latest/ratings
hadoop fs -mkdir /tmp/ml-latest/links
hadoop fs -mkdir /tmp/ml-latest/tags

#######################################################################################################################
Step 4: Copy the csv files into the HDFS directories:
#######################################################################################################################

hadoop fs -put /tmp/ml-latest/movies.csv /tmp/ml-latest/movies/.
hadoop fs -put /tmp/ml-latest/ratings.csv /tmp/ml-latest/ratings/.
hadoop fs -put /tmp/ml-latest/links.csv /tmp/ml-latest/links/.
hadoop fs -put /tmp/ml-latest/tags.csv /tmp/ml-latest/tags/.

#######################################################################################################################
Step 5: Download Hive CSV Serde in order to correctly parse CSV data
#######################################################################################################################

wget https://github.com/downloads/IllyaYalovyy/csv-serde/csv-serde-0.9.1.jar -o /tmp/csv-serde-0.9.1.jar

#######################################################################################################################
Step 6: Using Hive, load the CSV tables as Hive tables:
#######################################################################################################################

ADD jar /tmp/csv-serde-0.9.1.jar;

CREATE EXTERNAL TABLE IF NOT EXISTS temp_movies
    (movieId string, title string, genres string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION "/tmp/ml-latest/movies"
    TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS movies
    (movieId string, title string, genres string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    STORED AS ORC;

INSERT OVERWRITE TABLE movies SELECT * FROM temp_movies;

DROP TABLE temp_movies;

CREATE EXTERNAL TABLE IF NOT EXISTS temp_ratings
    (userId string, movieId string, rating float, rating_timestamp int)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION "/tmp/ml-latest/ratings"
    TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS ratings
    (userId string, movieId string, rating float, rating_timestamp int)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    STORED AS ORC;

INSERT OVERWRITE TABLE ratings SELECT * FROM temp_ratings;

DROP TABLE temp_ratings;

CREATE EXTERNAL TABLE IF NOT EXISTS temp_links
    (movieId string, imdbId string, tmdbId string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION "/tmp/ml-latest/links"
    TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS links
    (movieId string, imdbId string, tmdbId string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    STORED AS ORC;

INSERT OVERWRITE TABLE links SELECT * FROM temp_links;

DROP TABLE temp_links;

CREATE EXTERNAL TABLE IF NOT EXISTS temp_tags
    (userId string, movieId string, tag string, tag_timestamp int)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION "/tmp/ml-latest/tags"
    TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS tags
    (userId string, movieId string, tag string, tag_timestamp int)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    STORED AS ORC;

INSERT OVERWRITE TABLE tags SELECT * FROM temp_tags;

DROP TABLE temp_tags;

SHOW TABLES;

exit;




#ZEND
