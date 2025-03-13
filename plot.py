import csv
import matplotlib.pyplot as plt
import os


csv_dir = "codes/DE-Group-31-Project/results"
csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

for csv_file in csv_files:
    file_path = os.path.join(csv_dir, csv_file)
    data = {}

    # read data from csv file
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        headers = next(reader)
        cores = [int(header.split()[1]) for header in headers[1:]]

        for row in reader:
            mem = row[0]
            times = [float(time.split()[0]) for time in row[1:]]
            data[mem] = times

    # plot
    plt.figure()
    for mem, times in data.items():
        plt.plot(cores, times, marker='o', label=f"Memory {mem}")

    plt.xlabel('Cores')
    plt.ylabel('Time (seconds)')
    plt.title(f'Performance for {csv_file}')
    plt.legend()
    plt.grid(True)

    # save
    plot_file = os.path.join(csv_dir, f"{os.path.splitext(csv_file)[0]}.png")
    plt.savefig(plot_file)
    plt.close()

    print(f"Plot saved to {plot_file}")
