#!/bin/bash

hdfs dfs -rm -r /mututal
hdfs dfs -rm -r /output
hadoop com.sun.tools.javac.Main *.java
jar cf friendRec.jar *.class
hadoop jar friendRec.jar Drive input /mututal /output 5
hdfs dfs -cat /output/*
