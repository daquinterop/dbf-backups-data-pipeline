import pandas as pd
import numpy as np
import os
from dbfread import DBF, MissingMemoFile
import tomllib
import json
from pathlib import Path
import hashlib

with open("params.toml", "rb") as f:
    config = tomllib.load(f)
FILES_PATH = config["paths"]["path_raw_sacfa_tables"]
TABLES_THAT_MATTER = config["sacfa-pars"]["sacfa_tables"]
# if "fincas2014" in TABLES_THAT_MATTER:
#     TABLES_THAT_MATTER.remove("fincas2014")
MUST_TABLES = ["ubica", "lotesfin", "infculti", "insaplica"]

# dbf types to pandas dtypes generated in a previous step (dbf-dtypes.py)
with open('dbf-dtypes.toml', 'rb') as f:
    dtypes_config = tomllib.load(f)

# The log file generated in the previous step (dbf-move.py)
with open('logs/sacfa-move-log.json', 'r') as f:
    tables_log = json.load(f)


def dbf_table_to_pandas(dbf_path):
    try:
        table = DBF(
            dbf_path,
            encoding='ISO-8859-1'
        )
        df = pd.DataFrame.from_records(list(table.records))
    except MissingMemoFile:
        return
    return df

def create_32_char_id(input_string):
    encoded_string = input_string.encode('utf-8')    
    hash_obj = hashlib.md5(encoded_string)    
    return hash_obj.hexdigest()
            
# def join_on_map(df, map_df, join_col):
#     if map_df.empty:
#         return df
#     if df.empty:
#         return pd.DataFrame()
#     df = df.join(
#         map_df[map_df.columns.difference(df.columns)], 
#         on=join_col
#     )
#     return df

def rename_columns(df, table_name):
    if config['columns-rename'].get(table_name, False):
        df = df.rename(columns=config['columns-rename'][table_name])
    return df

if __name__ == '__main__':
    from tqdm import tqdm
    np.random.seed(2321)
    tables_log = np.random.choice(tables_log, size=10, replace=False)
    for backup in tqdm(tables_log):
        if not all(len(backup[t]) > 0 for t in MUST_TABLES):
            print(f"Skipping {backup['files_prefix']} because it doesn't have all must tables")
            continue

        backup_tables = {}
        dbfs_prefix = backup['files_prefix']
        for table in config['table']:
            dbf_path = Path(FILES_PATH) / f"{dbfs_prefix}-{table['base']['name']}.dbf"
            df = dbf_table_to_pandas(dbf_path)
            if df.empty:
                raise ValueError("Main table empty")
            df = rename_columns(df, table['base']['name'])
            df = df[table['base']['columns']]
            df = df.astype({
                c: dtypes_config[table['base']['name']][c]['type']
                for c in df.columns
            })
            join_tables = table.get('join', [])
            for table_join in join_tables:
                dbf_path = Path(FILES_PATH) / f"{dbfs_prefix}-{table_join['name']}.dbf"
                join_df = dbf_table_to_pandas(dbf_path)
                if join_df.empty:
                    raise ValueError("Join table empty")
                join_df = rename_columns(join_df, table_join['name'])
                join_df = join_df[table_join['columns']]
                join_df = join_df.astype({
                    c: dtypes_config[table_join['name']][c]['type']
                    for c in join_df.columns
                })
                join_df.columns = [
                    f"{table_join.get('prefix', '')}{c}" 
                    for c in join_df.columns
                ]
                df = df.join(
                    join_df.set_index(f"{table_join.get('prefix', '')}{table_join['idx']}"), 
                    on=table_join['on'],
                )
            # Create the index for this table. Index are not necessarily unique,
            # they are an identifier for the records of a unique entity that table
            # represent. For example, the Input use table represents a single application,
            # which can have several different inputs applied at once, e.g., a mix 
            # of pesticides.
            if table.get('idx', False):
                idxs = df[table['idx']].astype(str).apply(
                    lambda x: '-'.join(x), axis=1
                )
            else:
                idxs = df.index.astype(str)
            municipio, agronomo = backup['files_prefix'].split("-")[1:3]
            idxs = [
                create_32_char_id(f"{municipio}-{agronomo}-{i}") 
                for i in idxs
            ]
            df.index = idxs
            df = df.replace('', np.nan)
            backup_tables[table['name']] = df
            
        # Link tables by creating columns with indexes of the table to link.
        for table_name, links in config['relationships'].items():
            for link in links:
                df = backup_tables[table_name]
                map_df = backup_tables[link['link']]
                df[link['link']] = df[link['to']].map(
                    map_df.reset_index().set_index(link['from'])['index'].drop_duplicates()
                )
        continue