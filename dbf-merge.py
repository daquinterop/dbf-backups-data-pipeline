"""
Reads DBF files from a folder, processes the different tables to create unique
indexes, merge from other tables, and link tables by shared columns.

It reads several DBFs files, all in the same folder. Each group of tables are 
identified by a given prefix, which is the same for all tables of the same backup.
"""

import pandas as pd
import numpy as np
from dbfread import DBF, MissingMemoFile, DBFNotFound
import tomllib
import json
from pathlib import Path
import hashlib
import re
import ast
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

with open("params.toml", "rb") as f:
    config = tomllib.load(f)
FILES_PATH = config["paths"]["data_path"]
TABLES_THAT_MATTER = config["dbfs-pars"]["dbfs_tables"]
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
    except DBFNotFound:
        logger.warning(f"DBF file not found: {dbf_path}. Skipping this file.")
        return pd.DataFrame() 
    except MissingMemoFile:
        return
    return df

def create_32_char_id(input_string):
    encoded_string = input_string.encode('utf-8')    
    hash_obj = hashlib.md5(encoded_string)    
    return hash_obj.hexdigest()

# def rename_columns(df, table_props):
#     if config['columns-rename'].get(table_props['name'], False):
#         df = df.rename(columns=config['columns-rename'][table_props['name']])
#     return df

def subset_columns(df, table_props):
    cols = table_props['columns']
    try:
        return df[cols]
    except KeyError as e:
        error_keys = ast.literal_eval(re.search(r"\[.*\]", str(e)).group(0))
        if not all([i in table_props.get('column_keyerror_catch', [2]) for i in error_keys]):
            raise KeyError(e)
        logger.info(f"Columns not found in {table_props['name']}: {error_keys}")
        return df[[c for c in cols if c not in error_keys]]

def process_backup(backup):
    """
    Process the dbfs files of a single backup. It returns a dict with the tables. 
    The input is a dict with the backup information.
    """
    logger.info(f"Processing backup: {backup['files_prefix']}")
    if not all(len(backup[t]) > 0 for t in MUST_TABLES):
        logger.info(f"Skipping {backup['files_prefix']} because it doesn't have all must tables")
        return 

    backup_tables = {}
    dbfs_prefix = backup['files_prefix']
    for table in config['table']:
        dbf_path = Path(FILES_PATH) / 'tables-raw' / f"{dbfs_prefix}-{table['base']['name']}-1.dbf"
        df = dbf_table_to_pandas(dbf_path)
        mandatory_table = table['mandatory']
        if df.empty:
            if mandatory_table:
                logger.warning(f"Base table {dbf_path} is empty. Skipping this backup.")
                return
            else:
                logger.info(f"Table {table['base']['name']} is missing in backup {backup['files_prefix']}.")
                continue
        
        # df = rename_columns(df, table['base'])
        df = subset_columns(df, table['base'])
        df = df.astype({
            c: dtypes_config[table['base']['name']][c]['type']
            for c in df.columns
        })
        join_tables = table.get('join', [])
        for table_join in join_tables:
            dbf_path = Path(FILES_PATH) / "tables-raw" / f"{dbfs_prefix}-{table_join['name']}-1.dbf"
            join_df = dbf_table_to_pandas(dbf_path)
            if join_df.empty:
                logger.warning(f"Join table {dbf_path} is empty. Skipping join.")
                continue
            # join_df = rename_columns(join_df, table_join)
            join_df = subset_columns(join_df, table_join)
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
        if not table_name in backup_tables:
            continue
        for link in links:
            if not link['link'] in backup_tables:
                continue
            df = backup_tables[table_name]
            map_df = backup_tables[link['link']].copy()
            map_df = map_df.reset_index(drop=False)
            map_df.index = map_df[link['from']].astype(str).apply(lambda x: '-'.join(x), axis=1)
            df[link['link']] = \
                df[link['to']].astype(str).apply(lambda x: '-'.join(x), axis=1).map(
                map_df['index'].drop_duplicates()
            )
    return backup_tables

if __name__ == '__main__':
    # np.random.seed(2321)
    # tables_log = np.random.choice(tables_log, size=10, replace=False)
    np.random.shuffle(tables_log)
    backup_success_log = {}
    tables_dict = {}
    for backup in tables_log:
        if backup['files_prefix'] in config['control']['skip_backups']:
            logger.warning(f"Skipping backup {backup['files_prefix']} because it's in the skip list.")
            backup_success_log[backup['files_prefix']] = False
            continue
        backup_tables = process_backup(backup)
        if not backup_tables:
            backup_success_log[backup['files_prefix']] = False
            continue
        backup_success_log[backup['files_prefix']] = True
        for k, v in backup_tables.items():
            if not tables_dict.get(k, False):
                tables_dict[k] = []
            tables_dict[k].append(v)
            
    for table_name, tables in tables_dict.items():
        tables_dict[table_name] = pd.concat(tables)
        tables_dict[table_name]['tmp_col'] = tables_dict[table_name].index
        tables_dict[table_name] = tables_dict[table_name].drop_duplicates()
        tables_dict[table_name] = tables_dict[table_name].drop(columns=['tmp_col'])
        tables_dict[table_name].to_parquet(
            Path(FILES_PATH) / "tables-raw-parquet" / f"{table_name}.parquet", 
            index=True
        )
        logger.info(f"Saved table {table_name}")

    tables_dict['Farm'][['COFINCA']].to_csv("cofincas.csv", index=True)
    print()