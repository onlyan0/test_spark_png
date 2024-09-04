import numpy as np
import pandas as pd
from collections.abc import Callable
from typing import *
import argparse, yaml
import logging.config
from functools import wraps
import shutil

def log_dec(func):
    """
    A decorator function to log information about function calls.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        """
        The wrapper function that logs function calls
        """
        repr_args = [a for a in args if type(a) in (str, int, float)] + [f'{k}={v}' for k, v in kwargs.items() if type(v) in (str, int, float)]
        logging.info(f"Running {func.__name__} with params {repr_args}")
        try:
            result = func(*args, **kwargs)
            logging.info(f"Finished {func.__name__}")
        
        except Exception as e:
            logging.error(f"Error occurred in {func.__name__}: {e}")
            raise
        
        else:
            return result

    return wrapper

def parse_args() -> Callable:
    '''Parse input arguments, load configs from yaml'''
    parser = argparse.ArgumentParser(description='Preapare sale data for next week forecast')
    parser.add_argument('configfilepath', type=str
                        , help='Path to config file'
                        , nargs='?'
                        , default='config.yml'
                        , const='config.yml')
    parser.add_argument('loggingconfigfilepath', type=str
                        , help='Path to logging config file'
                        , nargs='?'
                        , default='logging_config.yml'
                        , const='logging_config.yml')

    args = parser.parse_args()

    with open(args.loggingconfigfilepath, 'r') as log_f, open(args.configfilepath, 'r') as f:
        logging_config = yaml.safe_load(log_f)
        config = yaml.safe_load(f)

    logging.config.dictConfig(logging_config)
    
    return config

@log_dec
def load_table(path: str, separator: str=',') -> pd.DataFrame:
    '''Load table to data frame'''

    return pd.read_csv(path, sep=separator)

@log_dec
def get_clean_and_transform_orders(orders_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Returns clean orders data
    1. Filter only delivered and shipped orders
    2. Filter orders with valid purchase date
    2. Extract year and week number of year from purchase_datetime
    '''
    clean_orders_df = orders_df[orders_df['order_status'].isin(["delivered","shipped"])] \
        .assign(order_purchase_timestamp=lambda x: pd.to_datetime(x['order_purchase_timestamp'])) \
        .query('not order_purchase_timestamp.isnull()') \
        .assign(year=lambda x: x['order_purchase_timestamp'].dt.year \
                , week = lambda x: x['order_purchase_timestamp'].dt.isocalendar().week
                , month = lambda x: x['order_purchase_timestamp'].dt.month \
                , day_of_week = lambda x: x['order_purchase_timestamp'].dt.dayofweek \
                )

    return clean_orders_df[['order_id', 'order_purchase_timestamp', 'year', 'week', 'month', 'day_of_week']]

@log_dec
def get_clean_and_transform_items(items_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Returns clean order items data
    '''

    return items_df[['order_id', 'product_id','seller_id', 'price']]

@log_dec
def get_clean_and_transform_products(products_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Returns clean products data
    '''

    return products_df[['product_id','product_category_name']]  

@log_dec
def get_joined_order_items_df(orders_df: pd.DataFrame, items_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Returns joined dataframe of orders, items and products
    '''
    return orders_df.merge(items_df, on='order_id').merge(products_df, on='product_id')

@log_dec
def write_data_to_parquet(df: pd.DataFrame, path: str, engine: str='fastparquet', compression: str='snappy') -> Callable:
    '''
    Writes result data into a parquet file
    '''
    if engine is None or engine == 'pyarrow':
        max_partitions = df['product_id'].nunique()
        if max_partitions > 1024:
            logging.warning('Raising max_partitions for pyarrow')
            kwargs = {'max_partitions': max_partitions}
    else:
        kwargs = {}
    shutil.rmtree(path, ignore_errors=True)
    logging.info("Folder removed")
    df.to_parquet(path=path, engine=engine, compression=compression, index=False, partition_cols=['product_category_name','product_id'], **kwargs)

def main() -> Callable:
    '''
    Load the orders data, transorm it and store as parquet
    '''
    config = parse_args()
    logger = logging.getLogger(config['logging_level'])

    files_config = config['files']
    #extract
    orders_df = load_table(**files_config['orders'])
    items_df = load_table(**files_config['items'])
    products_df = load_table(**files_config['products'])
    #transform
    orders_df = get_clean_and_transform_orders(orders_df)
    items_df = get_clean_and_transform_items(items_df)
    products_df = get_clean_and_transform_products(products_df)
    joined_df = get_joined_order_items_df(orders_df, items_df, products_df)
    #load
    write_data_to_parquet(joined_df, **files_config['output'])

if __name__ == '__main__':
    main()
