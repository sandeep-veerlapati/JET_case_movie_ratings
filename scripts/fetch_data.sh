#!/usr/bin/env bash

rm -rf /mnt/tmp/test_JET;
hdfs dfs -rm -R /tmp/JET

cd /mnt/tmp/;
mkdir -p test_JET/movies_csv/;
cd test_JET/movies_csv;
wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/ratings_Movies_and_TV.csv
hdfs dfs -mkdir -p /tmp/JET/movies_csv
hdfs dfs -put ratings_Movies_and_TV.csv /tmp/JET/movies_csv/
echo "Copied movies CSV file to hdfs location - /tmp/JET/movies_csv/ratings_Movies_and_TV.csv !!!"

cd /mnt/tmp/test_JET;
mkdir meta_movies;
cd meta_movies;
wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Movies_and_TV.json.gz
hdfs dfs -mkdir -p /tmp/JET/meta_movies
hdfs dfs -put meta_Movies_and_TV.json.gz /tmp/JET/meta_movies
echo "Copied movies meta JSON file to hdfs location - /tmp/JET/meta_movies/meta_Movies_and_TV.json.gz !!!"