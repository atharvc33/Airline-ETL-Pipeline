###########################################################################
# name        : main.py
# description : entry point - triggers all ETL operations
# version     : 0
# date        :
###########################################################################

# imports :

from pyspark.sql import SparkSession
from modules.airline import Airline
from modules.airport import Airport
from modules.plane import Plane
from modules.routes import Route

# modules          ← this is the FOLDER name
# .airline      ← this is the FILE name (airline.py)
# import Airline ← this is the CLASS name inside that file

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName('Airline_project').getOrCreate()

    # Define operations() function :-->>
    def operations():
        # ask user for Input:
        trigger = int(input("""
            1 for Airport
            2 for Airline
            3 for Plane
            4 for Route
        """))

        #
        if trigger == 1:
            obj = Airport()
            obj.airport_operation(spark)

        elif trigger == 2:
            obj = Airline()
            obj.airline_operation(spark)

        elif trigger == 3:
            obj = Plane()
            obj.plane_operation(spark)

        elif trigger == 4:
            obj = Route()
            obj.route_operation(spark)

        else:
            print("enter number between 1 - 4")

    # Calling function- operations()
    operations()