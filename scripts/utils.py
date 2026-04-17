import unicodedata
import logging 
from dbfread import DBF, DBFNotFound, MissingMemoFile
import os
from pathlib import Path
import pandas as pd

base_path = Path(__file__).resolve().parent.parent

def get_logger(kwargs=dict(level=logging.INFO,)):
    if 'filename' in kwargs:
        if not os.path.exists(base_path / 'logs/'):
            os.makedirs(base_path / 'logs/')
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        **kwargs
    )
    return logging.getLogger(__name__)

def remove_espanol(input_str):
    """Recplaces Spanish accents and special characters from a string."""
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def dbf_table_to_pandas(dbf_path, logger):
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