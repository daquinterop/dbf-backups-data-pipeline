"""
Gets the dtypes of the columns in the dbf tables and writes them to a text file. 
Useful for understanding the data structure and for later use in data processing.
"""
from dbfread import DBF
from pathlib import Path
import tomllib
import json
import pandas as pd
from utils import get_logger

with open("params.toml", "rb") as f:
    config = tomllib.load(f)
FILES_PATH = config["paths"]["data_path"]
TABLES_THAT_MATTER = config["dbfs-pars"]["dbfs_tables"]
base_path = Path(__file__).resolve().parent.parent
output_path = base_path / "output"

logger = get_logger()

with open(output_path / 'dbfs-moved-backups.json', 'r') as f:
    tables_log = json.load(f)

dtype_map = { # dbf types to pandas dtypes
    'C': 'string',
    'N': 'number',
    'L': 'bool',
    'D': 'datetime64[s]',
    'M': 'string',
    '0': "null",
    'F': 'number'
}

table_templates = {k: [] for k in TABLES_THAT_MATTER}
table_templates_count = {k: [] for k in TABLES_THAT_MATTER}
table_templates
for backup in tables_log:
    dbfs_prefix = backup['files_prefix']
    logger.info(f'Processing {dbfs_prefix} backup')
    for table_name in TABLES_THAT_MATTER:
        if len(backup[table_name]) == 0:
            continue
        dbf_path = Path(FILES_PATH) / "tables-raw" / f"{dbfs_prefix}-{table_name}-1.dbf"
        table = DBF(dbf_path, load=False)
        table_cols = {}
        for field in table.fields:
            dtype = dtype_map[field.type]
            if dtype == 'number': 
                dtype = 'Int64' if field.decimal_count == 0 else 'float'
            table_cols[field.name] = {'type': dtype, 'length': field.length}
        if (table_cols not in table_templates[table_name]) and (len(table_cols) > 0):
            table_templates[table_name].append(table_cols)
            table_templates_count[table_name].append(1)
        t_index = table_templates[table_name].index(table_cols)
        table_templates_count[table_name][t_index] += 1
        # table_templates[table_name][t_index]['count']["length"] += 1
    # break

table_templates_str = ''
table_dtypes_str = ''
for table_name, templates in table_templates.items():
    table_dtypes_str += f'[{table_name}]\n'
    table_templates_str += f'{"="*72}\n{table_name}\n{"="*72}\n'
    for n, template in enumerate(templates):
        table_templates_str += pd.DataFrame.from_dict(template).to_string()
        table_templates_str += f"\nCount: {table_templates_count[table_name][n]}"
        table_templates_str += '\n\n'
    table_templates_str += '\n\n'

    table_dtypes = {
        k: v for t in table_templates[table_name] for k, v in t.items()
    }
    for k, v in table_dtypes.items():
        table_dtypes_str += f"{k} = {{type = '{v['type']}', length = {v['length']}}}\n"
    table_dtypes_str += "\n"


with open(output_path / 'dbf-table-templates.txt', 'w') as f:
    f.write(table_templates_str)
    logger.info(f"Table templates written to {output_path / 'dbf-table-templates.txt'}")
with open(output_path / 'dbf-dtypes.toml', 'w') as f:
    f.write(table_dtypes_str)
    logger.info(f"Table dtypes written to {output_path / 'dbf-dtypes.toml.txt'}")