import pandas as pd 
import numpy as np
import tomllib
import logging
from pathlib import Path 
import os
from rapidfuzz.distance import JaroWinkler
from rapidfuzz.process import cdist

from utils import get_logger, remove_espanol

def string_homogenization(df:pd.DataFrame):
    """
    Homogenizes strings by: setting all to lowercase, removing special spanish 
    characters, replace spaces with underscores, collapsing spaces (underscores),
    and stripping additional spaces at the end and start.
    """
    df = df.copy()
    for col in df.select_dtypes('string').columns:
        df[col] = (df[col]
            # .str.normalize('NFC')
            .str.lower()
            .map(remove_espanol)
            .str.replace(r'[^a-z0-9]', '_', regex=True) 
            .str.replace(r'_+', '_', regex=True)        
            .str.strip('_')                            
        )
    return df

def get_similar_values(series:pd.Series):
    values_count = series.value_counts()
    values = series.dropna().unique()
    # values_match = {k: None for k in values_count}
    # for value, count in values_count.items(): 
    #     choices = set(values_count).difference([value])
    similarity_matrix = cdist(
        queries=values,
        choices=values,
        scorer=JaroWinkler.normalized_similarity,
    )
    similarity_matrix = np.where(similarity_matrix == 1, 0, similarity_matrix)

def get_subcategory_category_mapping(df:pd.DataFrame, col_subcategory:str, 
                                     col_category:str):
    """
    Creates a mapping from a subcategory to a category, resolving conflicts by 
    selecting the category value with the highest frequency. This is developed 
    thinking on the Input -> Management event map, e.g. Urea -> Fertilizer mngmt.
    """
    counts = df.groupby([col_subcategory, col_category]).size().reset_index(name='count')
    counts = counts.sort_values([col_subcategory, 'count'], ascending=[True, False])
    final_df = counts.drop_duplicates(subset=[col_subcategory])
    return dict(zip(final_df[col_subcategory], final_df[col_category]))

def fill_missing_category_from_subcategory(df:pd.DataFrame, 
                                           col_subcategory:str, col_category:str):
    df = df.copy()
    mapping = get_subcategory_category_mapping(df, col_subcategory, col_category)
    is_missing = df[col_category].isna()
    df.loc[is_missing, col_category] = df.loc[is_missing, col_subcategory].map(mapping)
    return df

functions_dict = dict(
    string_homogenization = string_homogenization,
    fill_missing_category_from_subcategory = fill_missing_category_from_subcategory
)

logger = get_logger(dict(
    filename='logs/data-clean.log',
    filemode='w',
    level=logging.INFO
))

base_path = Path(__file__).resolve().parent.parent
output_path = base_path / "output"

with open(base_path / "params.toml", "rb") as f:
    config = tomllib.load(f)
FILES_PATH = Path(config["paths"]["data_path"])
old_tables_path = FILES_PATH / "tables-raw-parquet"
new_tables_path = FILES_PATH / "tables-clean-parquet"

if not os.path.exists(new_tables_path):
    os.mkdir(new_tables_path)


if __name__ == '__main__':
    for table in config['table']:
        df = pd.read_parquet(old_tables_path / f"{table['name']}.parquet")
        cleaning_pipeline = table.get('cleaning', [])
        for pipeline_step in cleaning_pipeline:
            func = pipeline_step.get('function', False)
            logger.info(f"Applying {func} to {table['name']}")
            func = functions_dict[func]
            func_kwargs = pipeline_step.get('function_kwargs', {})
            if not func:
                raise ValueError("Function must be defined for each pipeline step")
            df = func(df, **func_kwargs)
        
        df.to_parquet(new_tables_path / f"{table['name']}.parquet")
        logger.info(f"Clean {table['name']} table saved to {new_tables_path / f"{table['name']}.parquet"}")


    logger.info("Done")