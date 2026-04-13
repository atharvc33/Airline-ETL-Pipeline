# routes.py

# importing :

import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql import functions as F

# importing window function:
from pyspark.sql.window import Window


# importing all functions from utility module :

from modules.utility import read_csv,good_bad_data,write_data


# creating class :
class Route:
    def route_operation(self,spark:SparkSession):

        # creating df by reading csv file:(header exist - no toDF needed!)
        df = read_csv(spark,"data/raw_data/Route")

        # null checks on(airline,sec_iairport,dest_airport):
        check_cols =["airline","src_airport","dest_airport"]
        good_df,bad_df = good_bad_data(df,check_cols)

        # add load_date columns
        good_df = good_df.withColumn("load_date",F.current_date())

        # Write good/bad data :
        write_data(good_df,"data/processed_data/good_data/Routes",file_format="parquet")
        write_data(bad_df,"data/processed_data/bad_data/Routes",file_format="csv")

        # SCD 1 : partition by airline, src_airport, dest_airport

        # gold path writing:
        gold_path = "data/gold_data/Routes"

        if os.path.exists(gold_path) and len([f for f in os.listdir(gold_path) if f.endswith('.parquet')]) > 0:
            prev_data = spark.read.parquet(gold_path)
            union_df = prev_data.union(good_df)

            # creating windowspec:
            windowspec = Window.partitionBy("airline","src_airport","dest_airport").orderBy(F.col("load_date").desc())

            # creating rank :
            union_df = union_df.withColumn("rank",F.row_number().over(windowspec))

            #  selecting latest data (rank ==1 )

            scd1 = union_df.filter(F.col("rank") == 1).drop("rank")

        else:
            scd1 =good_df

            # final Write :

        write_data(scd1,gold_path,file_format="parquet")