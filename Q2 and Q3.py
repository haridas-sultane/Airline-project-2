from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("Q3").getOrCreate()
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    from pyspark.sql.window import *
    def replace(info):
        if info=="\\N":
            return "(unknown)"
        elif info is None:
            return "(unknown)"
        else:
            return info

    createUdf= udf(lambda x: replace(x))
    airlineDf = spark.read.csv(r"D:\pyspark file\airline data\airline.csv", header=True,
                               schema="airlineId integer,Name string,Alias string,IATA string,ICAO string,Callasign string,Country string,Active string")
    # airlineDf.show()
    airlineDf.withColumn("Alias",createUdf(col("Alias")))\
        .withColumn("IATA",createUdf(col("IATA"))).show()

    newairlineDf =airlineDf.na.fill(-1).na.fill("(unknown)")

    airportDf=spark.read.csv(r"D:\pyspark file\airline data\airport.csv",schema="Airport_ID integer,Name string,City string,Country string,IATA string,ICAO string,Latitude double,Longitude double,Altitude long,Timezone string,DST string,Tz string,Type string,Source string")
    # airportDf.withColumn("IATA",createUdf(col("IATA"))).show(500)

    routesDf=spark.read.parquet(r"D:\pyspark file\airline data\routes.snappy.parquet")
    routesNew=routesDf.withColumn("airline_id", when((col("airline_id")=="\\N"),"-1").otherwise(col("airline_id")))\
                        .withColumn("codeshare",createUdf(col("codeshare"))).\
                        withColumn("dest_airport_id", when((col("dest_airport_id")=="\\N"),"-1").otherwise(col("dest_airport_id")))
    # routesNew.show()

    # routesDf.write.csv(r"C:\Users\harid\PycharmProjects\pythonProject1\converted\routes air",header=True)

    ##read file from written csv file
    newrouteDf=spark.read.csv(r"C:\Users\harid\PycharmProjects\pythonProject1\converted\routes air",header=True)
    newrouteDf2=newrouteDf.withColumn("airline_id",(col("airline_id")).cast(IntegerType()))\
        .withColumn("src_airport_id",(col("src_airport_id")).cast(IntegerType()))\
        .withColumn("dest_airport_id", (col("dest_airport_id")).cast(IntegerType()))

    newrouteDf2 = newrouteDf2.na.fill(-1).na.fill("(unknown)")
    newrouteDf2.write.csv(r"C:\Users\harid\PycharmProjects\pyspark pro2\airline project file\new route")



    # newrouteDf2.show()
    # newrouteDf2.printSchema()


    # planeDf = spark.read.options(delimiter="").csv(r"D:\pyspark file\airline data\plane.csv", header=True,inferSchema=True)
    '''either use sep or delimeter ??? by default delimeter and sep is (",") in csv file '''
    planeDf = spark.read.csv(r"D:\pyspark file\airline data\plane.csv", header=True,inferSchema=True,sep="")
    # planeDf.show()
    # planeDf.printSchema()
    planeNew=planeDf.withColumn("IATA Code",createUdf(col("IATA Code"))).withColumn("ICAO Code",createUdf(col("ICAO Code")))
    # planeNew.show()

    #Q2)
    print("Q.2:-")
    airlineDf.join(airportDf,airlineDf.Country==airportDf.Country, "inner").filter(airlineDf.Active=="Y").\
        select(airportDf.Country).distinct().show()

    #Q.. 3A)
    print("Q.3:-")
    new = routesNew.groupBy("airline_id", "src_airport").count()
    new.show()
    #
    alt=new.join(airlineDf, airlineDf.airlineId == new.airline_id, "inner").filter(col("count") > 3).distinct().select(airlineDf.Name,
                                                                                                     new.src_airport,new.airline_id)