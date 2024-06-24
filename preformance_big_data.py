import os
import random
import string

def generate_random_text_line(min_length=50, max_length=80):
    length = random.randint(min_length, max_length)
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_initial_file(file_name, target_size_mb=100):
    target_size_bytes = target_size_mb * 1024 * 1024
    with open(file_name, 'w') as file:
        current_size = 0
        while current_size < target_size_bytes:
            line = generate_random_text_line() + '\n'
            file.write(line)
            current_size += len(line)
    print(f"Initial file of {target_size_mb}MB created as {file_name}")

def expand_file_to_target_size(file_name, target_size_gb=3):
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024
    while os.path.getsize(file_name) < target_size_bytes:
        with open(file_name, 'rb') as file:
            file_content = file.read()
        with open(file_name, 'ab') as file:
            file.write(file_content)
        print(f"File expanded to {os.path.getsize(file_name) / (1024 * 1024)} MB")

if __name__ == "__main__":
    initial_file_name = "3GB.txt"
    generate_initial_file(initial_file_name, target_size_mb=100)
    expand_file_to_target_size(initial_file_name, target_size_gb=3)
    print(f"Final file size: {os.path.getsize(initial_file_name) / (1024 * 1024 * 1024)} GB")
