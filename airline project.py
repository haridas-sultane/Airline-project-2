from pyspark.sql import SparkSession,Row
if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("project").getOrCreate()
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    from pyspark.sql.window import *

    airportDf=spark.read.csv(r"D:\pyspark file\airline data\airport.csv",schema="Airport_ID integer,Name string,City string,Country string,IATA string,ICAO string,Latitude double,Longitude double,Altitude long,Timezone string,DST string,Tz string,Type string,Source string")
    # airportDf.show()

    newrouteDf = spark.read.csv(r"C:\Users\harid\PycharmProjects\pythonProject1\converted\routes air", header=True)
    newrouteDf2 = newrouteDf.withColumn("airline_id", (col("airline_id")).cast(IntegerType())).withColumn(
        "src_airport_id", (col("src_airport_id")).cast(IntegerType())). \
        withColumn("dest_airport_id", (col("dest_airport_id")).cast(IntegerType()))

    newrouteDf2=newrouteDf2.na.fill(-1).na.fill("(unknown)")

    routes=newrouteDf2.withColumn("value",lit(int(1)))

    takeOff = routes.groupBy("src_airport").agg(sum("value").alias("takeoff")).distinct()
    landing= routes.groupBy("dest_airport").agg(sum("value").alias("landing")).distinct()
    airport_detail=takeOff.join(landing,takeOff.src_airport == landing.dest_airport,"inner")
    # airport_detail.show()

    maxAirport=airport_detail.select("src_airport",lit(col("takeoff") + col("landing")).alias("total"))\
    .withColumn("rank", rank().over(Window.orderBy(col("total").desc()))).filter(col("rank") == 1).select(col("src_airport"))
    print("Q.4 ans:-")


    # airportDf.join(maxAirport,airportDf.IATA==maxAirport.src_airport,"inner").drop("src_airport").show()
    #######  OR #####################
    # airportDf.filter(airportDf.IATA.isin(maxAirport.rdd.flatMap(lambda x:x).collect())).show()


    newrouteDf2.groupBy("src_airport").count().alias("source_airport").union(newrouteDf2.groupBy("dest_airport").count()).show()

    minAirport =airport_detail.select("src_airport", lit(col("takeoff") + col("landing")).alias("total"))\
        .withColumn("rank", rank().over(Window.orderBy(col("total").asc()))).filter(col("rank") == 1).select(col("src_airport"))
    # print(minAirport.show())
    # print("Q.3 ans:-")
    airportDf.join(minAirport,col("IATA") == minAirport.src_airport, "inner").drop("src_airport").show()

    ##### or######
    ## compare with rdd by using isin() method
    # airportDf.filter(airportDf.IATA.isin(minAirport.rdd.flatMap(lambda x: x).collect())).show()

    ######## or #######################
    # minimum = minAirport.rdd.flatMap(lambda x: x).collect()
    # airportDf.filter(airportDf.IATA.isin(minimum)).show()




    # takeOff = newrouteDf2.groupBy("src_airport").count().distinct().orderBy(col)
    # takeOff.show()
    # landing = newrouteDf2.groupBy("dest_airport").agg(count(col("dest_airport")).alias("landing")).distinct().orderBy(col("landing").asc())
    # landing.show()
    # airport_detail = takeOff.join(landing, takeOff.src_airport == landing.dest_airport, "inner").orderBy(col("takeoff").asc())
    # airport_detail.show()
    # minAirport =airport_detail.select("src_airport", lit(col("takeoff") + col("landing")).alias("total")).orderBy(col("total").asc())\
    #     .withColumn("rank", rank().over(Window.orderBy(col("total").asc()))).filter(col("rank") == 1).select(col("src_airport"))
    # # minAirport.show()
    # print(minAirport.count())

    # minAirport = airport_detail.select("src_airport", lit(col("takeoff") + col("landing")).alias("total")).orderBy(
    #     col("total").asc()) \
    #     .withColumn("rank", rank().over(Window.orderBy(col("total").asc()))).filter(col("rank") == 1).select(
    #     col("src_airport"))















    # join1 = join.select("src_airport", lit(col("takeoff") + col("landing")).alias("total")).orderBy(col("total").desc())
    # join1.withColumn("rank",rank().over(Window.orderBy(col("total").desc()))).filter(col("rank")==1).show()
    # join1.withColumn("rank", rank().over(Window.orderBy(col("total").desc()))).filter(col("rank") == 1).select(col("src_airport")).show()
    # join1.show()