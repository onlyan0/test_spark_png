import unittest, pytest, io
import pandas as pd
from pathlib import Path

import sys
path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

import src.main.main as f_main

@pytest.fixture(scope='session')
def setup_data_orders():
    print("\nSetting up orders...")
    testdata_orders_not_clean = '''"order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"
"556bbf53c2c22fbb9ef31a414dd444a6","5883f965ac70043c2e908c3657c5d548",delivered,2017-12-21 17:43:41,2017-12-21 17:56:24,2017-12-26 16:59:04,2018-01-04 21:56:01,2018-01-22 00:00:00
"36530871a5e80138db53bcfd8a104d90","4dafe3c841d2d6cc8a8b6d25b35704b9",cancelled,2017-05-09 11:48:37,2017-05-11 11:45:14,2017-05-11 13:21:47,,2017-06-08 00:00:00
b2bd1f09c1b8a4a36940f6f6e49d5dec,"8ed14e450a6268ec13f325e7c2eafeed",approved,2018-02-02 16:15:41,2018-02-02 16:36:26,2018-02-06 01:28:26,2018-02-20 18:09:29,2018-03-07 00:00:00'''
    
    test_orders_df = pd.read_csv(filepath_or_buffer = io.StringIO(testdata_orders_not_clean), sep=',')

    yield test_orders_df 

@pytest.fixture(scope='session')
def setup_data_clean_orders():
    print("\nSetting up orders...")
 
    test_orders_df = pd.DataFrame({'order_id': {0: '556bbf53c2c22fbb9ef31a414dd444a6'},
                            'order_purchase_timestamp': {0: pd.Timestamp('2017-12-21 17:43:41')},
                            'year': {0: 2017},
                            'week': {0: 51},
                            'month': {0: 12},
                            'day_of_week': {0: 3}}) \
                .astype(dtype= {"year":"int32", "week":"UInt32", "month": "int32", "day_of_week": "int32"})

    yield test_orders_df 


@pytest.fixture(scope='session')
def setup_data_items():
    print("\nSetting up items...")
    testdata_items = '''"order_id","product_id","seller_id","price"
"556bbf53c2c22fbb9ef31a414dd444a6","9e2d3a8d8ffad53e2e35282a2020221c","1da366cade6d8276e7d8beea7af5d4bf", 49.90'''    
    test_items_df = pd.read_csv(filepath_or_buffer = io.StringIO(testdata_items), sep=',')

    yield test_items_df 


@pytest.fixture(scope='session')
def setup_data_products():
    print("\nSetting up products...")
    testdata_products = '''"product_id","product_category_name"
"9e2d3a8d8ffad53e2e35282a2020221c",moveis_decoracao'''    
    test_products_df = pd.read_csv(filepath_or_buffer = io.StringIO(testdata_products), sep=',')

    yield test_products_df 


def test_get_clean_and_transform_orders(setup_data_orders, setup_data_clean_orders):

    orders_df = setup_data_orders
    orders_df = f_main.get_clean_and_transform_orders(orders_df)

    res_df = setup_data_clean_orders
    pd.testing.assert_frame_equal(res_df, orders_df)


def test_get_joined_order_items_df(setup_data_clean_orders, setup_data_items, setup_data_products):

    orders_df, items_df, products_df = setup_data_clean_orders, setup_data_items, setup_data_products
    test_df = f_main.get_joined_order_items_df(orders_df, items_df, products_df)
    print(test_df.to_dict())

    res_df = pd.DataFrame({'order_id': {0: '556bbf53c2c22fbb9ef31a414dd444a6'},
                            'order_purchase_timestamp': {0: pd.Timestamp('2017-12-21 17:43:41')},
                            'year': {0: 2017},
                            'week': {0: 51},
                            'month': {0: 12},
                            'day_of_week': {0: 3},
                            'product_id': {0: '9e2d3a8d8ffad53e2e35282a2020221c'},
                            'seller_id': {0: '1da366cade6d8276e7d8beea7af5d4bf'},
                            'price': {0: 49.9},
                            'product_category_name': {0: 'moveis_decoracao'}}) \
                .astype(dtype= {"year":"int32", "week":"UInt32", "month": "int32", "day_of_week": "int32"})
    
    

    pd.testing.assert_frame_equal(res_df, test_df)
