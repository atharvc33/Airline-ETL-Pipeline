import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# importing functions from another file:
from modules.utility import read_csv, good_bad_data, write_data

from pyspark.sql.types import StringType

class Airport:
    def airport_operation(self,spark:SparkSession):
        columns=["airport_id","name","city","country","iata","icao","latitude",
                 "longitude","altitude","timezone","dst","tz_database","type","source"]

        df = read_csv(spark,"data/raw_data/Airport")
        df = df.toDF(*columns)

        # step 2 - business rule (negative airline_id = bad data)
        negative_df = df.filter(F.col("airport_id") <0 )
        good_df = df.filter(F.col("airport_id")>=0)

        # Step 3 - Extra Step A - transform DST codes to full names
        good_df = good_df.withColumn("dst",
            F.when(F.col("dst") == "E","Europe")
            .when(F.col("dst") == "A","US/Canada")
            .when(F.col("dst") == "S","South America")
            .when(F.col("dst") == "O","Australia")
            .when(F.col("dst")=="Z","New Zealand")
            .otherwise("Unknown")
        )

        #  Extra step B - Fix null timezones -->>
        # replacing null & "\\N" values from "tz_database" col and val are not null or \\N then keeping as it is.

        good_df = good_df.withColumn("tz_database",
            F.when(
                (F.col("tz_database").isNull()) | (F.col("tz_database") == "\\N"),
                "IST"
            ).otherwise(F.col("tz_database"))
        )


        # Step 4 - null checks (name,city,country)

        # Why These 3 Columns? ^^^
        #  name -> every aiport must have a name
        # city -> airport city must needs to know
        # country -> needed for regulations and reporting

        check_cols =["name","city","country"]
        good_df,null_df = good_bad_data(good_df,check_cols)

        #  step 5 - union/combine bad dataframe
        final_bad_df = negative_df.union(null_df)

        # Step 6 - add load date (to track when data is loaded)
        good_df = good_df.withColumn("load_date",F.current_date())

        # Auditing — who loaded?, what?, when?

        # SCD1 logic — finding the latest version of a record
        # Debugging — tracing problems back to specific loads


        # Step 7 - write good/bad data -->>
        write_data(good_df,"data/processed_data/good_data/Airport",file_format="parquet")
        write_data(final_bad_df,"data/processed_data/bad_data/Airport",file_format="csv")

        # step 8 - SCD1 logic-->>
        # Step 7 - SCD1 logic (keep only latest record per airport_id)
        gold_path ="data/gold_data/Airport"

        # Step 1 → Union old data + new data
        #          (stack them together)
        # Step 2 → Rank rows per airline_id by date
        #          (newest gets rank 1)
        # Step 3 → Keep only rank 1
        #          (latest record wins)

        # Creating window_spec -
        # window: for each airport_id,order by load_date descending
        window_spec = Window.partitionBy("airport_id").orderBy(F.col("load_date").desc())


        if os.path.exists(gold_path) and len([f for f in os.listdir(gold_path) if f.endswith('.parquet')]) > 0:
            # previous data exists-> union old+new->keep latest

            # read into  memory first using cache!
            prev_df = spark.read.parquet(gold_path).cache()
            prev_df.count()       # force actual reads into memory

            union_df = prev_df.union(good_df)

            # rank finding -->>
            union_df = union_df.withColumn("rank",F.row_number().over(window_spec))

            # SCD1 with rank 1-->>
            scd1 = union_df.filter(F.col("rank") == 1).drop("rank")

            # first load (save good_df directly)-->>
        else:
            scd1 = good_df


        # Write final scd1 data to gold layer -->>
        write_data(scd1,gold_path,file_format="parquet")

        # Bronze  → raw data as is (your raw_data folder)
        # Silver  → cleaned good/bad data (your processed_data folder)
        # Gold    → final business ready data (SCD1 result)


