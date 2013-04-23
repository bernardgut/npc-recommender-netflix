#!/bin/sh
echo "Removing Files..."
rm -r classes/*
rm netfliz.jar
rm -r results
hadoop dfs -rmr std11/output
hadoop dfs -rmr std11/input
echo "*********************"
echo "Puting input..."
hadoop dfs -put std11/input std11/input
echo "*********************"
echo "Compiling..."
javac -cp lib/hadoop-mapred-0.21.0.jar:lib/hadoop-common-0.21.0.jar -source 1.6 -target 1.6 -d classes src/ch/epfl/advdb/*.java
jar cvf netfliz.jar -C classes .
echo "*********************"
echo "Starting Hadoop task..."
hadoop jar netfliz.jar ch.epfl.advdb.Main std11/input std11/output 
echo "*********************"
echo "recovering resuts..."
hadoop dfs -get std11 results
echo "*********************"

