"""
Move the .dbf files we need from the raw backup folders to a new folder, renaming 
them in the process to have a more consistent naming convention. The backup folders 
have the next structure:

    raw_data_path
    ├── backup_batch
    │   └── user_backup_folder
            └── .dbf files
"""
import tomllib
import sys
import os
import zipfile
from pathlib import Path
import tqdm
import re 
import shutil
import json
import unicodedata

with open("params.toml", "rb") as f:
    config = tomllib.load(f)
raw_data_path = config["paths"]["raw_data_path"]
path_raw_dbfs_tables = config["paths"]["data_path"]
base_path = Path(__file__).resolve().parent.parent

dbfs_folders = config["dbfs-pars"]["dbfs_folders"]
dbfs_tables = config["dbfs-pars"]["dbfs_tables"]

def in_tables_we_want(file):
    for table_name in dbfs_tables:
        if table_name.lower() == 'fincas':
            if file.name.lower().startswith(table_name.lower()):
                return table_name
        if table_name.lower() == file.stem.lower():
            return table_name
    return None

def remove_espanol(input_str):
    """Recplaces Spanish accents and special characters from a string."""
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


if not os.path.exists(base_path / 'logs/'):
    os.makedirs(base_path / 'logs/')
agronomos_log = {} # A log of agronomos for each municipio
tables_log = [] # A log for each backup folder

for dbfs_folder in dbfs_folders:
    dbfs_folder_path = Path(raw_data_path) / dbfs_folder
    for backup_folder in tqdm.tqdm(dbfs_folder_path.iterdir()):
        tables_map = {table_name: [] for table_name in dbfs_tables}
        
        municipio = backup_folder.name.split("_")[0]
        municipio = municipio.replace("_", " ").strip().replace(" ", "_").lower()
        agronomo = "_".join(backup_folder.name.split("_")[1:])
        backup_number = re.findall(r'\d+', agronomo)
        backup_number = int(backup_number[0]) if len(backup_number) > 0 else 1
        agronomo = re.sub(r'\d', '', agronomo)
        agronomo = agronomo.replace("_", " ").strip().replace(" ", "_").lower()

        agronomo = remove_espanol(agronomo)
        municipio = remove_espanol(municipio)
        
        if agronomos_log.get(municipio, False):
            if agronomo not in agronomos_log[municipio]:
                agronomos_log[municipio].append(agronomo)
        else:
            agronomos_log[municipio] = [agronomo]
        

        for file in backup_folder.iterdir():
            if file.suffix.lower() != ".dbf":
                continue
            table_name = in_tables_we_want(file)
            if table_name:
                tables_map[table_name].append(file)

        files_prefix = f"{dbfs_folder.lower()}-{municipio}-{agronomo}-{backup_number}"
        tables_log.append({
            "dbfs_folder": dbfs_folder,
            "backup_folder": backup_folder.name,
            "files_prefix": files_prefix,
        })

        for k, v in tables_map.items():
            tables_log[-1][k] = [i.name for i in v]
            for n, file in enumerate(v, start=1):
                new_file_name = f"{files_prefix}-{k}-{n}.dbf"
                new_file_path = Path(path_raw_dbfs_tables) / 'tables-raw' / new_file_name
                shutil.copy(file, new_file_path)

total_backups_count = len(tables_log)
all_tables_in_backups_count = sum([all([len(t[k]) > 0 for k in dbfs_tables]) for t in tables_log])
print(f"Total backups processed: {total_backups_count}")
print(f"Backups with all tables: {all_tables_in_backups_count} ({all_tables_in_backups_count/total_backups_count:.2%})")

with open(base_path / 'logs' / 'sacfa-move-log.json', 'w') as f:
    json.dump(tables_log, f, indent=4)

with open(base_path / 'logs' / 'acfa-agronomos-log.json', 'w') as f:
    json.dump([agronomos_log], f, indent=4)