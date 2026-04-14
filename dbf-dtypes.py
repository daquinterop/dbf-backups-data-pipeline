"""
Gets the dtypes of the columns in the dbf tables and writes them to a text file. 
Useful for understanding the data structure and for later use in data processing.
"""
from dbfread import DBF
from pathlib import Path
import tomllib
import json

with open("params.toml", "rb") as f:
    config = tomllib.load(f)
FILES_PATH = config["paths"]["path_raw_sacfa_tables"]
TABLES_THAT_MATTER = config["sacfa-pars"]["sacfa_tables"]

with open('logs/sacfa-move-log.json', 'r') as f:
    tables_log = json.load(f)

dtype_map = { # dbf types to pandas dtypes
    'C': 'string',
    'N': 'number',
    'L': 'bool',
    'D': 'datetime64[s]',
    'M': 'string',
    '0': "null"
}

filelines = []
for backup in tables_log:
    if not all(len(backup[t]) > 0 for t in TABLES_THAT_MATTER):
        continue
    dbfs_prefix = backup['files_prefix']
    for table_name in TABLES_THAT_MATTER:
        dbf_path = Path(FILES_PATH) / f"{dbfs_prefix}-{table_name}.dbf"
        table = DBF(dbf_path, load=False)
        filelines.append(f"[{table_name}]")
        for field in table.fields:
            dtype = dtype_map[field.type]
            if dtype == 'number': 
                dtype = 'Int64' if field.decimal_count == 0 else 'float'
            filelines.append(f"{field.name} = {{type = '{dtype}', length = {field.length}}}")
        filelines.append("\n")
    break

with open('dbf-dtypes.toml', 'w') as f:
    for line in filelines: f.write(line + "\n")

for l in filelines: print(l)

