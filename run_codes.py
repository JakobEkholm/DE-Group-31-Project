import time
import subprocess
import shlex
import csv

executor_memory = [3, 6]
executor_cores = [2, 4]

master_url = "spark://192.168.2.39:7077"
spark_script = "codes/DE-Group-31-Project/test.py"
data_size =["100", "all"]
master_ip = "192.168.2.39"
workers_ip=["192.168.2.128", "192.168.2.34", "192.168.2.45", "192.168.2.143", "192.168.2.216"]
file_path = "codes/DE-Group-31-Project/results/"

def generate_spark_cmd(data_size, mem, cores):
    return (
        f"spark-submit "
        f"--master {master_url} "
        f"--executor-memory {str(mem)+'g'} "
        f"--executor-cores {cores} "
        f"--conf spark.app.name=Reddit{data_size}_mem={mem}g,cores={cores} "
        f"{spark_script} --data_size {data_size}"
    )

def start_worker(i):
    # start workers
    cmd_start_worker = f"ssh {workers_ip[i]} /home/ubuntu/spark/sbin/start-worker.sh spark://{master_ip}:7077"
    print(f"==== start workers: {cmd_start_worker} ====")
    process = subprocess.run(shlex.split(cmd_start_worker), capture_output=True, text=True)
    if process.returncode == 0:
      workers_number = i+1
      print(f"workers number: {workers_number}")
    else:
      print(f"failed: {process.stderr}\n")
      raise Exception("failed to start workers")

def execute_spark(size, mem, cores):
    name = f"Reddit{size}_mem={mem}g,cores={cores}"
    cmd = generate_spark_cmd(size, mem, cores)

    print(f"==== config: {name} ====")

    start_time = time.time()
    process = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
    end_time = time.time()

    elapsed = end_time - start_time

    if process.returncode == 0:
      print(f"success: {elapsed:.2f} seconds\n")
    else:
      print(f"failed: {elapsed:.2f} seconds\n")
      print(f"info: {process.stderr}\n")

    return elapsed


if __name__ == "__main__":
    i = 0
    for size in data_size:
        start_worker(i)
        csv_file_name = f"Reddit{size}_Workers{i+1}_performance.csv"
        csv_file = file_path + csv_file_name
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            header = ["Executor Memory (GB)"] + [f"Cores {cores}" for cores in executor_cores]
            writer.writerow(header)

            for mem in executor_memory:
                row = [f"{mem}g"]
                for cores in executor_cores:
                    elapsed = execute_spark(size, mem, cores)
                    row.append(f"{elapsed:.2f} seconds")

                writer.writerow(row)

        print(f"result saved to{csv_file}")
