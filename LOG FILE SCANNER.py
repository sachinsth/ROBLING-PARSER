import os
import glob



path1 = "F:/log file HT/Extraction-HT July04"

os.chdir(path1)


def read_file(file_path):
    with open(file_path, 'r') as f:
        for line in f.readlines():
            line = line.strip()
            print(line)


for files in os.listdir():
    if files.endswith(".log"):

        file_path = f"{path1}/{files}"
        print(file_path)
        # read_file(file_path)



