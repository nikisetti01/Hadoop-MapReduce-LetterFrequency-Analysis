import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from hdfs import InsecureClient
import re

# Configura il client HDFS
hdfs_url = 'http://10.1.1.114:9870'  # URL del tuo HDFS
hdfs_log_dir = '/user/hadoop/results/300MB.txt'
client = InsecureClient(hdfs_url, user='hadoop')

# Funzione per estrarre dati dai log Hadoop
def extract_data_from_log(log_file):
    data = []
    current_entry = None
    try:
        with client.read(log_file) as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Errore durante la lettura del file {log_file}: {e}")
        return data
    
    for line in lines:
        line = line.decode('utf-8').strip()
        print(f"Processing line: {line}")  # Debug
        
        # Identificare l'inizio di un nuovo job (LetterCount o LetterFrequency)
        if "Running LetterCount job" in line or "Running LetterFrequencycombiner job" in line:
            # Aggiungi il job precedente se presente
            if current_entry:
                data.append(current_entry)
            
            current_entry = {
                'job_type': 'LetterCount' if "Running LetterCount job" in line else 'LetterFrequency',
                'log_file': log_file
            }
        
        # Estrarre le metriche
        if current_entry:
            exec_time_match = re.search(r'Total time spent by all (maps|reduces) in occupied slots \(ms\)=\d+', line)
            map_time_match = re.search(r'Total time spent by all maps in occupied slots \(ms\)=(\d+)', line)
            reduce_time_match = re.search(r'Total time spent by all reduces in occupied slots \(ms\)=(\d+)', line)
            memory_match = re.search(r'Physical memory \(bytes\) snapshot=(\d+)', line)
            vcore_ms_match = re.search(r'Total vcore-milliseconds taken by all tasks=(\d+)', line)
            gc_time_match = re.search(r'GC time elapsed \(ms\)=(\d+)', line)
            cpu_time_match = re.search(r'CPU time spent \(ms\)=(\d+)', line)
            map_input_records_match = re.search(r'Map input records=(\d+)', line)
            map_output_records_match = re.search(r'Map output records=(\d+)', line)
            reduce_output_records_match = re.search(r'Reduce output records=(\d+)', line)

            if exec_time_match:
                if 'execution_time' not in current_entry:
                    current_entry['execution_time'] = 0
                current_entry['execution_time'] += int(re.findall(r'\d+', exec_time_match.group(0))[0]) / 1000.0  # Convert ms to seconds
                print(f"Execution time extracted: {current_entry['execution_time']} seconds")  # Debug
            
            if map_time_match:
                current_entry['map_time'] = int(map_time_match.group(1)) / 1000.0  # Convert ms to seconds
                print(f"Map time extracted: {current_entry['map_time']} seconds")  # Debug

            if reduce_time_match:
                current_entry['reduce_time'] = int(reduce_time_match.group(1)) / 1000.0  # Convert ms to seconds
                print(f"Reduce time extracted: {current_entry['reduce_time']} seconds")  # Debug

            if memory_match:
                current_entry['memory_usage'] = int(memory_match.group(1)) / (1024 ** 3)  # Convert bytes to GB
                print(f"Memory usage extracted: {current_entry['memory_usage']} GB")  # Debug
            
            if vcore_ms_match:
                current_entry['vcore_ms'] = int(vcore_ms_match.group(1))
                print(f"vcore-ms extracted: {current_entry['vcore_ms']}")  # Debug

            if gc_time_match:
                current_entry['gc_time'] = int(gc_time_match.group(1)) / 1000.0  # Convert ms to seconds
                print(f"GC time extracted: {current_entry['gc_time']} seconds")  # Debug

            if cpu_time_match:
                current_entry['cpu_time'] = int(cpu_time_match.group(1)) / 1000.0  # Convert ms to seconds
                print(f"CPU time extracted: {current_entry['cpu_time']} seconds")  # Debug

            if map_input_records_match:
                current_entry['map_input_records'] = int(map_input_records_match.group(1))
                print(f"Map input records extracted: {current_entry['map_input_records']}")  # Debug

            if map_output_records_match:
                current_entry['map_output_records'] = int(map_output_records_match.group(1))
                print(f"Map output records extracted: {current_entry['map_output_records']}")  # Debug

            if reduce_output_records_match:
                current_entry['reduce_output_records'] = int(reduce_output_records_match.group(1))
                print(f"Reduce output records extracted: {current_entry['reduce_output_records']}")  # Debug

            size_mode_match = re.search(r'results/(\w+)/(\w+)/(\d+)reducers/logs/', log_file)
            if size_mode_match:
                current_entry['job_size'] = size_mode_match.group(1)
                current_entry['mode'] = size_mode_match.group(2)
                current_entry['reducers'] = int(size_mode_match.group(3))
                print(f"Job size: {current_entry['job_size']}, Mode: {current_entry['mode']}, Reducers: {current_entry['reducers']}")  # Debug

    # Aggiungi l'ultimo entry se non è stato aggiunto
    if current_entry:
        data.append(current_entry)
    
    return data

# Funzione per ottenere tutti i file di log dalla directory su HDFS
def list_log_files(hdfs_dir):
    log_files = []
    try:
        directories = client.list(hdfs_dir)
        print(f"Directories in {hdfs_dir}: {directories}")  # Debug
        for directory in directories:
            directory_path = f"{hdfs_dir}/{directory}"
            print(f"Checking {directory_path}")  # Debug
            status = client.status(directory_path)
            if status['type'] == 'DIRECTORY':
                log_files.extend(list_log_files(directory_path))
            elif status['type'] == 'FILE' and directory.endswith('.log'):
                log_files.append(directory_path)
    except Exception as e:
        print(f"Errore durante la lettura della directory {hdfs_dir}: {e}")
    return log_files

# Ottieni tutti i file di log dalla directory principale su HDFS
log_files = list_log_files(hdfs_log_dir)

all_data = []

for log_file in log_files:
    print(f"Processing file: {log_file}")  # Debug
    data = extract_data_from_log(log_file)
    print(f"Data extracted from {log_file}: {data}")  # Debug
    all_data.extend(data)

# Debug: verifica il contenuto di all_data
print(f"All data: {all_data}")

# Converti i dati in un DataFrame di pandas
df = pd.DataFrame(all_data)

# Debug: controlla se 'execution_time' è presente
print(f"DataFrame columns: {df.columns}")

if 'execution_time' not in df.columns:
    print("execution_time not found in DataFrame columns")
    print(df.head())
else:
    # Filtra i dati con esecuzioni valide
    df = df[df['execution_time'].notnull()]

# Salva il DataFrame in un file CSV (opzionale)
output_path = '/tmp/hadoop_logs_summary_300mb.csv'
try:
    df.to_csv(output_path, index=False)  # Usa /tmp/ o un altro percorso sicuro
    print(f"File salvato con successo in {output_path}")
except PermissionError:
    print(f"Permessi insufficienti per salvare il file in {output_path}")


