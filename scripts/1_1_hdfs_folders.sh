#!/bin/bash -x

#Create HDFS directories. Create separate drectories for each dataset for eacy handling of the data
hdfs dfs -mkdir /user/vijaya/data
hdfs dfs -mkdir /user/vijaya/data/raw
hdfs dfs -mkdir /user/vijaya/data/decomposed
hdfs dfs -mkdir /user/vijaya/data/modelled
hdfs dfs -mkdir /user/vijaya/data/schema
hdfs dfs -mkdir /user/vijaya/data/raw/airports
hdfs dfs -mkdir /user/vijaya/data/raw/carriers
hdfs dfs -mkdir /user/vijaya/data/raw/planedate
hdfs dfs -mkdir /user/vijaya/data/raw/otp
hdfs dfs -mkdir /user/vijaya/data/decomposed/airports
hdfs dfs -mkdir /user/vijaya/data/decomposed/carriers
hdfs dfs -mkdir /user/vijaya/data/decomposed/planedate
hdfs dfs -mkdir /user/vijaya/data/decomposed/otp
hdfs dfs -mkdir /user/vijaya/data/modelled/airports
hdfs dfs -mkdir /user/vijaya/data/modelled/carriers
hdfs dfs -mkdir /user/vijaya/data/modelled/planedate
hdfs dfs -mkdir /user/vijaya/data/modelled/otp
hdfs dfs -mkdir /user/vijaya/data/schema/airports
hdfs dfs -mkdir /user/vijaya/data/schema/carriers
hdfs dfs -mkdir /user/vijaya/data/schema/planedate
hdfs dfs -mkdir /user/vijaya/data/schema/otp