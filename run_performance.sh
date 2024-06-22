#!/bin/bash

# Directory contenente i file di input in HDFS
input_dir="/user/hadoop/"

# Directory di output in HDFS
hdfs_output_dir="/user/hadoop/results"

# Array dei nomi dei file di input
input_files=("3KB.txt" "3MB.txt")

# Array delle dimensioni corrispondenti ai file di input
sizes=("3KB" "3MB")

# Array delle modalità
modes=("Combiner" "InMapper")

# Percorso completo di Hadoop
HADOOP_CMD="/opt/hadoop/bin/hadoop"

# Funzione per eseguire Hadoop e salvare i log direttamente su HDFS
run_hadoop() {
  local mode=$1
  local size=$2
  local input_file=$3
  local hdfs_mode_dir="$hdfs_output_dir/$size/$mode"
  local jar_path="target/lettercount-1.0-SNAPSHOT.jar"
  local main_class="it.unipi.hadoop.Start"
  local hdfs_log_file="$hdfs_mode_dir/logs/log_$(date +%Y%m%d_%H%M%S).log"

  # Crea la directory HDFS per i risultati e i log
  echo "Creazione della directory HDFS $hdfs_mode_dir..."
  hdfs dfs -mkdir -p $hdfs_mode_dir/logs

  echo "Esecuzione di LetterCount e LetterFrequency ($mode) per $size..."
  $HADOOP_CMD jar $jar_path $main_class \
    "$input_dir/$input_file" "$hdfs_mode_dir/output_$size_$mode" 1 "$mode" \
    2>&1 | hdfs dfs -appendToFile - $hdfs_log_file

  if [ $? -ne 0 ]; then
    echo "Errore durante l'esecuzione di Hadoop per $mode con $size. Vedi $hdfs_log_file per i dettagli."
    exit 1
  fi
}

# Esegui per ciascun file di input e modalità
for ((i=0; i<${#input_files[@]}; i++)); do
  input_file=${input_files[$i]}
  size=${sizes[$i]}

  for mode in "${modes[@]}"; do
    run_hadoop $mode $size $input_file
  done
done

echo "Tutti i lavori sono stati completati. I risultati e i log sono stati salvati su HDFS nella cartella $hdfs_output_dir."

