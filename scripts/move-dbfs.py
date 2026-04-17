"""
Move the .dbf files we need from the raw backup folders to a new folder, renaming 
them in the process to have a more consistent naming convention. The backup folders 
have the next structure:

    raw_data_path
    ├── backup_batch
    │   └── user_backup_folder {admin_unit}-{user_name}-{backup_number}
            └── .dbf files
"""
import tomllib
import os
from pathlib import Path
import tqdm
import re 
import shutil
import json
from utils import remove_espanol, get_logger

logger = get_logger()

with open("params.toml", "rb") as f:
    config = tomllib.load(f)
raw_data_path = config["paths"]["raw_data_path"]
path_raw_dbfs_tables = config["paths"]["data_path"]
base_path = Path(__file__).resolve().parent.parent
output_path = base_path / "output"
if not os.path.exists(output_path):
    os.makedirs(output_path)

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

users_log = {} # A log of users for each admin_unit
tables_log = [] # A log for each backup folder

for dbfs_folder in dbfs_folders:
    dbfs_folder_path = Path(raw_data_path) / dbfs_folder
    for backup_folder in tqdm.tqdm(dbfs_folder_path.iterdir()):
        tables_map = {table_name: [] for table_name in dbfs_tables}
        
        admin_unit = backup_folder.name.split("_")[0]
        admin_unit = admin_unit.replace("_", " ").strip().replace(" ", "_").lower()
        user = "_".join(backup_folder.name.split("_")[1:])
        backup_number = re.findall(r'\d+', user)
        backup_number = int(backup_number[0]) if len(backup_number) > 0 else 1
        user = re.sub(r'\d', '', user)
        user = user.replace("_", " ").strip().replace(" ", "_").lower()

        user = remove_espanol(user)
        admin_unit = remove_espanol(admin_unit)
        
        if users_log.get(admin_unit, False):
            if user not in users_log[admin_unit]:
                users_log[admin_unit].append(user)
        else:
            users_log[admin_unit] = [user]
        

        for file in backup_folder.iterdir():
            if file.suffix.lower() != ".dbf":
                continue
            table_name = in_tables_we_want(file)
            if table_name:
                tables_map[table_name].append(file)

        files_prefix = f"{dbfs_folder.lower()}-{admin_unit}-{user}-{backup_number}"
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
logger.info(f"Total backups processed: {total_backups_count}")
logger.info(f"Backups with all tables: {all_tables_in_backups_count} ({all_tables_in_backups_count/total_backups_count:.2%})")

with open(output_path / 'dbfs-moved-backups.json', 'w') as f:
    json.dump(tables_log, f, indent=4)

with open(output_path / 'dbfs-users.json', 'w') as f:
    json.dump([users_log], f, indent=4)