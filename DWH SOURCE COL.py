import glob
import re
from sql_metadata import Parser
import pandas as pd
import os

files = (glob.glob('F:/roblingdaas/Log/f_cus_ord_ln_ld_20211126154311.log'))


def extract_seperations(file_path: str):
    with open(file_path, 'r') as f:
        variables = []
        variables_2 = []
        variables_3 = []
        final_line = ""
        final_line_2 = ""
        final_line_3 = ""
        final_variable = []
        load_into_temp = False
        load_into_temp1 = False
        load_into_temp2 = False
        for line in f.readlines():
            line = line.strip()
            if line.startswith('INSERT INTO DW_TMP.'):
                TEMP_TABLE = line.split('.')[1].split(' (')[0]
                variables.append(line)
                load_into_temp = True
            elif load_into_temp:
                variables.append(line)
                if line.startswith('SELECT'):
                    variables_2.append(line)
                    variables.remove(line)
                    final_line = ('\n').join(variables)
                    final_variable.append(final_line)
                    load_into_temp = False
                    load_into_temp1 = True
            elif load_into_temp1:
                if line.startswith('--,'):
                    continue
                variables_2.append(line)
                if line.startswith('FROM'):
                    variables_3.append(line)
                    variables_2.remove(line)
                    final_line_2 = ('\n').join(variables_2)
                    load_into_temp1 = False
                    load_into_temp2 = True
            elif load_into_temp2:
                variables_3.append(line)
                if 'Snowflake Query ID' in line:
                    variables_3.remove(line)
                    final_line_3 = ('\n').join(variables_3)
                    load_into_temp2 = False
        return final_line, final_line_2, final_line_3


for f in files:
    (line1, line2, line3) = extract_seperations(f)

    line1 = line1.split('(')[1]
    line1 = line1.split(')')[0]
    line1 = line1.split(',')

    line2 = line2.split('SELECT')[1]

    line2 = line2.split('\n,')

line3 = []
for elements in line2:
    line3.append(elements.replace('\n', ''))

for elements in line3:

    if 'END) ' in elements:
        a = line3.index(elements)

        line3[a] = elements.split('END) ')[1]
for elements in line3:
    if 'END ' in elements:
        a = line3.index(elements)

        line3[a] = elements.split('END ')[1]
for elements in line3:
    if ')' in elements:
        a = line3.index(elements)

        line3[a] = elements.split(') ')[1]
for elements in line3:
    if '.' in elements:
        a = line3.index(elements)

        line3[a] = elements.split('.')[1]
for elements in line3:
    if ' ' in elements:
        a = line3.index(elements)

        line3[a] = elements.split(' ')[1]

corresponding_col = {}
for key in line1:
    for value in line3:
        key = key.replace('\n', '')
        corresponding_col[key] = value
        line3.remove(value)
        break

print(corresponding_col)

