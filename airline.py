# This is where the actual ETL logic lives for airline data.

# 1. Read CSV           → using utility.read_csv()
# 2. Transform data     → clean, rename, add columns
# 3. Split good/bad     → using utility.good_bad_data()
# 4. Write results      → using utility.write_data()

###############################################################################

import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# importing functions from another file:
from modules.utility import read_csv, good_bad_data, write_data

from pyspark.sql.types import StringType


#  airline.py needs a class with a method called airline_operation.

class Airline:
    def airline_operation(self,spark:SparkSession):

        # step 1 - define column names manually(no header in csv)
        columns = ["airline_id","name","alias","iata","icao","callsign","country","active"]
        df = read_csv(spark,"data/raw_data/Airline/airline.csv")
        df = df.toDF(*columns)

        # step 2 - business rule: negative airline_id = bad data
        bad_negative_df = df.filter(F.col("airline_id")<0)
        good_df = df.filter(F.col("airline_id") >=0)

        # step 3 - generate alias from name
        good_df = good_df.withColumn(
                "alias",
                        F.expr(
                        "aggregate(split(name, ' '), '', (acc, x) -> concat(acc, substring(x, 1, 1)))")
                                     )

        # step 4 - Null Checks (seperate good and bad based on null columns) -->>
        cols_to_check = ["name","iata","country"]
        good_df,bad_null_df = good_bad_data(good_df,cols_to_check)

        # combine both bad dataframes into one :
        final_bad_df = bad_negative_df.union(bad_null_df)

        # Why These 3 Columns? ^^^
        #  name -> every airline must have a name
        # iata -> industry standard code — needed for routing
        # country -> needed for regulations and reporting


        # Step 5 - Add load_date (tracks when data was loaded)
        good_df = good_df.withColumn("load_date",F.current_date())

        # (load_date) is Essential for:

        # Auditing — who loaded what when
        # SCD logic — finding the latest version of a record
        # Debugging — tracing problems back to specific loads


        # step 6 - write good data as parquet, bad data as csv:

        write_data(good_df,"data/processed_data/good_data/Airline",file_format ="parquet")
        write_data(final_bad_df,"data/processed_data/bad_data/Airline",file_format="csv")


        # SCD - ( Slowly changing Dimension )
        # Step 7 - SCD1 logic (keep only latest record per airline_id)
        gold_path = "data/gold_data/Airline"

        # Step 1 → Union old data + new data
        #          (stack them together)
        # Step 2 → Rank rows per airline_id by date
        #          (newest gets rank 1)
        # Step 3 → Keep only rank 1
        #          (latest record wins)


        # window: for each airline_id,order by load_date descending
        window_spec= Window.partitionBy("airline_id").orderBy(F.col("load_date").desc())

        # checks if folder exists AND has parquet files inside :
        if os.path.exists(gold_path) and len([f for f in os.listdir(gold_path) if f.endswith('.parquet')]) > 0:
            # previous data exists-> union old+new->keep latest
            prev_df = spark.read.parquet(gold_path)
            union_df = prev_df.union(good_df)

            # rank finding
            union_df = union_df.withColumn("rank",F.row_number().over(window_spec))
            # scd1 with rank == 1
            scd1_df = union_df.filter(F.col("rank") == 1).drop("rank")
        else:
            # first load -> save good_df directly
            scd1_df = good_df

        # write final scd1 data to gold layer:

        write_data(scd1_df,gold_path,file_format = "parquet")

        # Bronze  → raw data as is (your raw_data folder)
        # Silver  → cleaned good/bad data (your processed_data folder)
        # Gold    → final business ready data (SCD1 result)

