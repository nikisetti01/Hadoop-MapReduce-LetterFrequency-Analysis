#!/bin/bash
 
# Compilazione e packaging del progetto Maven
mvn clean package
 
# Copia del file JAR generato sul server remoto
scp target/lettercount-1.0-SNAPSHOT.jar hadoop@10.1.1.114:
# Esecuzione del job Hadoop
hadoop jar target/lettercount-1.0-SNAPSHOT.jar it.unipi.hadoop.Start pg100.txt output 
# Visualizzazione del contenuto della directory di output
hadoop fs -ls output
 
# Visualizzazione dei primi 10 risultati
hadoop fs -cat output/part* | head