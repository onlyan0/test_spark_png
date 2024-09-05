# test_spark_png

This is a test exercise for Nikolai Bulatov

We take 3 csv datasets as an input (orders, order items and products), transform it and write into an output parquet file

## Data

Data is taken from here
[here](https://urldefense.com/v3/__https:/www.kaggle.com/datasets/olistbr/brazilian-ecommerce__;!!GF_29dbcQIUBPA!0eOFLZ_-uouGUR1R58CnSad4oYkpTvpQO-knJ6q4Tm2RWOX-bvFOQvApkY9GTOqz_-gnROXtz2bWvH9guhY$)

## Prerequisites

- Python

```cmd
pip3 install -r requisites.txt
```
## Configure and run

Configures are in [config.yml](config.yml)

Either change it or pass another yml file to python program as an argument

```cmd
pyton3 src/main/main.py --configfilepath another_config.yml
```

In config file you can set logging level, paths to 3 input csv datasets (orders, order items and products) and path to output parquet dataset