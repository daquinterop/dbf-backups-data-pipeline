# Some scripts to process DBFs files from desktop software backups.

These scripts are to solve a specific problem. Consider the case of a desktop-based data-collection platform. This platform receives input data from an user and store it locally in DBF files. All users create a backup of their data, by zipping those dbfs files. We have several instances or batches of those backups. Then, the DBF files are stored following the next path structure:

```
raw_data_path
    ├── backup_batch_year_1
    │   └── user_1_backup_folder
    │   │   └── .dbf files
    │   └── user_2_backup_folder
    │       └── .dbf files
    ....
    ── backup_batch_year_2
    │   └── user_1_backup_folder
    │       └── .dbf files
    │   └── user_2_backup_folder
    │       └── .dbf files
```

The scripts take the parameters defined in `params.toml`, and process the files to create the tables as defined in that same file.

There scripts are meant to be run in the next order:

1. **move-dbfs.py**: moves the all DBFs files from the user_backup_folder, to a given folder. At the end, all dbfs are in the same folder and are assigned names given a defined naming convention (e.g. f"{backup_batch}-{user}-{table}.dbf") 
2. **dbf-dtypes.py**: creates a .toml with the pandas datatypes of the dbfs tables we are interested in. 
3. **dbf-merge.py**: takes the dbfs files and dtypes from the previous steps, creates the tables as defined in the `params.toml` file, and saves them as a parquet file.