import glob
from sql_metadata import Parser
import pandas as pd
import os

files = (glob.glob('F:/roblingdaas/Log/f_cus_ord_ln_ld_20211126154311.log'))

def extract_seperations(file_path:str):
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
                load_into_temp=True
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
                if 'Snowflake Query ID' in line:
                    variables_2.remove(line)
                    final_line_2=('\n').join(variables_2)
                    load_into_temp1 = False
        return TEMP_TABLE,final_line_2

for f in files:
     sql = ""
     temp_table, sql = extract_seperations(f)
     print(temp_table)

     a=[]
     b=[]


     parser = Parser(sql)
     print(parser.tables_aliases)
     print(parser.columns_aliases)
