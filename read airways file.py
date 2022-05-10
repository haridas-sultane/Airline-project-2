from pyspark.sql import SparkSession,Row
if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("airline pro").getOrCreate()
    from pyspark.sql.types import *
    from pyspark.sql.functions import *

    routesDf=spark.read.parquet(r"D:\pyspark file\airline data\routes.snappy.parquet")
    # routesDf.printSchema()
    routesDf.show()
    # routesDf.write.parquet(r"C:\Users\harid\PycharmProjects\pyspark pro2\airline project file\routes")
    # routesDf.write.csv(r"C:\Users\harid\PycharmProjects\pyspark pro2\airline project file\routes\csv routes")

    airlineDf=spark.read.csv(r"D:\pyspark file\airline data\airline.csv",header=True,
                              schema="airlineId integer,Name string,Alias string,IATA string,ICAO string,Callasign string,Country string,Active string")

    airlineDf.show()

    # df2.write.csv(r"C:\Users\harid\PycharmProjects\pyspark pro2\airline project file\df2")

    # airlineDf.printSchema()

    # airlineDf.write.csv(r"C:\Users\harid\PycharmProjects\pyspark pro2\airline project file\airline",sep="\t")

    airportDf=spark.read.csv(r"D:\pyspark file\airline data\airport.csv",schema="Airport_ID integer,Name string,City string,Country string,IATA string,ICAO string,Latitude float,Longitude float,Altitude long,Timezone long,DST string,Tz string,Type string,Source string")
    airportDf.show()

    # airportDf.write.csv(r"C:\Users\harid\PycharmProjects\pyspark pro2\airline project file\airport")

    planeDf=spark.read.options(delimiter="").csv(r"D:\pyspark file\airline data\plane.csv",header=True,inferSchema=True)
    planeDf.show()

    # planeDf.write.csv(r"C:\Users\harid\PycharmProjects\pyspark pro2\airline project file\plane csv")

    # airlineDf.groupBy(air)select("airlineId","Name").

    new=routesDf.groupBy("airline_id","src_airport").count()
    # new.show()

    # new.join(airlineDf, airlineDf.airlineId==new.airline_id ,"inner").filter(new.count > 3).select(airlineDf.Name,new.airline_id).show()

    takeOff=routesDf.groupBy("airline_id","src_airport").count().alias("take_off")
    takeOff.show()