#!/bin/bash

# Directory contenente i file di input in HDFS
input_dir="/user/hadoop/"

# Directory di output in HDFS
hdfs_output_dir="/user/hadoop/results"

# Array dei nomi dei file di input
input_files=("300MB.txt")

# Array delle dimensioni corrispondenti ai file di input
sizes=("300MB.txt")

# Array delle modalità
modes=("Combiner" "InMapper")

# Array del numero di riduttori
reducers=(3 5 10)  # Puoi aggiungere altri valori se necessario

# Percorso completo di Hadoop
HADOOP_CMD="/opt/hadoop/bin/hadoop"

# Funzione per eseguire Hadoop e salvare i log direttamente su HDFS
run_hadoop() {
  local mode=$1
  local size=$2
  local input_file=$3
  local reducers=$4
  local hdfs_mode_dir="$hdfs_output_dir/$size/$mode/${reducers}reducers"
  local jar_path="target/lettercount-1.0-SNAPSHOT.jar"
  local main_class="it.unipi.hadoop.Start"
  local hdfs_log_file="$hdfs_mode_dir/logs/log_$(date +%Y%m%d_%H%M%S).log"

  # Crea la directory HDFS per i risultati e i log
  echo "Creazione della directory HDFS $hdfs_mode_dir..."
  #hdfs dfs -mkdir -p $hdfs_mode_dir/logs

  echo "Esecuzione di LetterCount e LetterFrequency ($mode) per $size con $reducers reducer(s)..."
  $HADOOP_CMD jar $jar_path $main_class \
    "$input_dir/$input_file" "$hdfs_mode_dir/output_$size_${mode}_${reducers}reducers" $reducers "$mode" \
    2>&1 | hdfs dfs -appendToFile - $hdfs_log_file

  if [ $? -ne 0 ]; then
    echo "Errore durante l'esecuzione di Hadoop per $mode con $size e $reducers reducer(s). Vedi $hdfs_log_file per i dettagli."
    exit 1
  fi
}

# Esegui per ciascun file di input, modalità e numero di reducers
for ((i=0; i<${#input_files[@]}; i++)); do
  input_file=${input_files[$i]}
  size=${sizes[$i]}

  for mode in "${modes[@]}"; do
    for reducer in "${reducers[@]}"; do
      run_hadoop $mode $size $input_file $reducer
    done
  done
done

echo "Tutti i lavori sono stati completati. I risultati e i log sono stati salvati su HDFS nella cartella $hdfs_output_dir."
