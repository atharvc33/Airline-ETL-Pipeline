# plane.py

import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql import functions as F

# importing window-->>
from pyspark.sql.window import Window

# importing functions from utility.py:
from modules.utility import read_csv,good_bad_data,write_data

from pyspark.sql.types import StringType

# Creating Class :
class Plane:
    def plane_operation(self,spark:SparkSession):

        # creating dataframe from plane.csv : (header is present no need to use toDF)
        df = read_csv(spark,"data/raw_data/Plane")

        # Rename Columns :
        df = df.withColumnRenamed("Name","name")\
               .withColumnRenamed("IATA Code","iata_code")\
               .withColumnRenamed("ICAO Code","icao_code")

        # null checks om (name,iata_code,icao_code):
        col_checks = ["name","iata_code","icao_code"]
        good_df,bad_df = good_bad_data(df,col_checks)

        # add load date :

        good_df = good_df.withColumn("load_date",F.current_date())

        # Write good/bad Data :

        write_data(good_df,"data/processed_data/good_data/Plane",file_format="parquet")
        write_data(bad_df,"data/processed_data/bad_data/Plane",file_format="csv")


        # SCD1 (Partition by name this time and order by load_date):

        # path of gold_data:

        gold_path ="data/gold_data/Plane"

        # creating windowspec:
        windowspec = Window.partitionBy("name").orderBy(F.col("load_date").desc())

        #  union prev_data and good_df (by checking if exist):

        if os.path.exists(gold_path) and len([f for f in os.listdir(gold_path) if f.endswith('.parquet')]) > 0:
           # read into memory first using cache !
            prev_data = spark.read.parquet(gold_path).cache()
            prev_data.count()             # force to read from cache
            union_df = prev_data.union(good_df)

            # rank with window function :

            union_df = union_df.withColumn("rank",F.row_number().over(windowspec))

            # filter to rank == 1 :

            scd1 = union_df.filter(F.col("rank") == 1).drop("rank")

        else:
            scd1 = good_df


        # final write :

        write_data(scd1,gold_path,file_format ="parquet")
