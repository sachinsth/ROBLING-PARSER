import glob
from sql_metadata import Parser
import pandas as pd
import os


def extraction_parser(file_path: str):
    with open(file_path, 'r') as f:
        var = []
        var_2 = []
        s_line = ""
        f_line = ""
        flag1 = False
        flag2 = False
        flag3 = False
        for line in f.readlines():
            line = line.strip()
            if "# Exporting Data #" in line:
                flag1 = True
            if flag1:
                if "SELECT" in line or "WITH" in line:
                    flag1 = False
                    if line.startswith('2022'):
                        var.append(line.split(': ')[1])
                    else:
                        var.append(line)
                    flag2 = True
            elif flag2:
                if line.startswith('--'):
                    continue
                var.append(line)
                if 'Data file:' in line:
                    var.remove(line)
                    f_line = ('\n').join(var)
                    flag2 = False
        return f_line


def extract_filename(file_path: str):
    with open(file_path, 'r') as f:
        filename = ""
        for line in f.readlines():
            line = line.strip()
            if 'WHERE LOWER(SCRIPT_NAME)' in line:
                filename = line
        filename = filename.split("=LOWER('")[1]
        filename = filename.split("'")[0]
        return filename


path1 = "F:/Extraction - HT JAN02"

os.chdir(path1)

for files in os.listdir():
    if files.endswith(".log"):
        file_path = f"{path1}/{files}"
        filename = extract_filename(files)
        print(filename)

        select_from = extraction_parser(files)
        parser = Parser(select_from)
        parser.tables_aliases
        print(parser.columns_aliases)
