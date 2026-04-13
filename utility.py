###########################################################################
# name        : utility.py
# description : common/shared helper functions for all modules
# version     : 0
# date        :
###########################################################################

from pyspark.sql import SparkSession,DataFrame
from pyspark.sql import functions as F


# # func: read_csv
# # purpose: reading csv file.creating a dataframe sing csv.
# # params: spark, filepath

def read_csv(spark: SparkSession, filepath:str) -> DataFrame:
    df = spark.read.option("header",True).option("inferSchema",True).csv(filepath)
    return df


# 💡 Why no SparkSession creation here? —>
# utility.py will never be run directly. It will be imported by airline.py, airport.py etc.
# Those modules get spark from main.py and pass it in. utility.py just uses whatever spark it receives.


# func for seperating good data & bad data -->>

# what does this function need as input?
# 1. The DataFrame to check
# 2. A list of column names to check for nulls

# # func: good_bad_data
# # purpose: seperate 2 dataframes as good_df & bad_df
# # params: df, cols

def good_bad_data(df: DataFrame,cols: list) -> tuple:

    # build condition: all columns in cols must be not null
    condition = F.col(cols[0]).isNotNull()

    for col in cols[1:]:
        condition = condition & F.col(col).isNotNull()

    good_df = df.filter(condition)
    bad_df = df.filter(~condition)

    return good_df, bad_df


# func: write_data
# purpose: saves a DataFrame to disk as CSV
# params: df, filepath, mode

def write_data(df:DataFrame, filepath:str, mode:str ="overwrite",file_format:str="csv") -> None:
    if file_format == "csv":
        df.write.mode(mode).option("header",True).csv(filepath)
    elif file_format == "parquet":
        df.write.mode(mode).parquet(filepath)

# mode: str = "overwrite":
# Default value — if caller doesn't specify a mode, overwrite is used automatically
# -> None
# This function returns nothing — it just saves to disk, job done