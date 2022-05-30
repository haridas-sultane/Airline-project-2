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
    airlineDf.show()
    # airlineDf.withColumn("Alias",createUdf(col("Alias")))\
    #     .withColumn("IATA",createUdf(col("IATA"))).show()

    newairlineDf =airlineDf.na.fill(-1).na.fill("(unknown)")

    airportDf=spark.read.csv(r"D:\pyspark file\airline data\airport.csv",schema="Airport_ID integer,Name string,City string,Country string,IATA string,ICAO string,Latitude double,Longitude double,Altitude long,Timezone string,DST string,Tz string,Type string,Source string")


    # airportDf.withColumn("IATA",createUdf(col("IATA"))).show()

    routesDf=spark.read.parquet(r"D:\pyspark file\airline data\routes.snappy.parquet")
    routesNew=routesDf.withColumn("airline_id", when((col("airline_id")=="\\N"),"-1").otherwise(col("airline_id")))\
                        .withColumn("codeshare",createUdf(col("codeshare"))).\
                        withColumn("dest_airport_id", when((col("dest_airport_id")=="\\N"),"-1").otherwise(col("dest_airport_id")))
    routesNew.show()

    routesDf.write.csv(r"C:\Users\harid\PycharmProjects\pythonProject1\converted\routes air",header=True)

    ##read file from written csv file
    newrouteDf=spark.read.csv(r"C:\Users\harid\PycharmProjects\pythonProject1\converted\routes air",header=True)
    newrouteDf2=newrouteDf.withColumn("airline_id",(col("airline_id")).cast(IntegerType()))\
        .withColumn("src_airport_id",(col("src_airport_id")).cast(IntegerType()))\
        .withColumn("dest_airport_id", (col("dest_airport_id")).cast(IntegerType()))

    newrouteDf2 = newrouteDf2.na.fill(-1).na.fill("(unknown)")
    newrouteDf2.write.csv(r"C:\Users\harid\PycharmProjects\pyspark pro2\airline project file\new route")



    # newrouteDf2.show()
    # newrouteDf2.printSchema()


    planeDf = spark.read.options(delimiter="").csv(r"D:\pyspark file\airline data\plane.csv", header=True,inferSchema=True)
    '''either use sep or delimeter ??? by default delimeter and sep is (",") in csv file '''
    planeDf = spark.read.csv(r"D:\pyspark file\airline data\plane.csv", header=True,inferSchema=True,sep="")
    # planeDf.show()
    # planeDf.printSchema()
    planeNew=planeDf.withColumn("IATA Code",createUdf(col("IATA Code"))).withColumn("ICAO Code",createUdf(col("ICAO Code")))


    # print(planeDf.count())
    # planeNew.show()


    Q2)
    print("Q.2:-")
    airlineDf.join(airportDf,airlineDf.Country==airportDf.Country, "inner").filter(airlineDf.Active=="Y").\
        select(airportDf.Country).distinct().show()

    ### Q.. 3A)
    # print("Q.3:-")
    new = routesNew.groupBy("airline_id", "src_airport").count()
    new.show()
    alt=new.join(airlineDf, airlineDf.airlineId == new.airline_id, "inner").filter(col("count") > 3).\
        select(airlineDf.Name,new.src_airport,new.airline_id).distinct()
    print(alt.count())




    ###q.6)
    dataF=routesNew.join(airlineDf,airlineDf.airlineId==routesNew.airline_id,"inner").filter(routesNew.stops==0).\
        withColumn("row_num",row_number().over(Window.orderBy(airlineDf.airlineId))).select(airlineDf.airlineId,
                                                                                            airlineDf.Name,routesNew.src_airport,
                                                                                            routesNew.dest_airport,col("row_num"))
    # dataF.show()
    # print(dataF.count())
    dataF2=dataF.join(airportDf,dataF.src_airport==airportDf.IATA,"left").withColumn("src_airport_name",lit(airportDf.Name)).\
        select(col("airlineId"),dataF.Name,col("src_airport_name"),col("row_num"))
    # dataF2.show()
    # print(dataF2.count())
    # dataF3=dataF.join(airportDf, dataF.dest_airport == airportDf.IATA,"left").withColumn("dest_airport_name",lit(airportDf.Name)). \
    #     select(col("airlineId"), dataF.Name, col("dest_airport_name"),col("row_num"))
    # dataF3.show()
    # print(dataF3.count())
    # dataF4=dataF2.join(dataF3,on="row_num").select(dataF2.airlineId,dataF2.Name,col("src_airport_name"),col("dest_airport_name"))
    # dataF4.show()
    # print(dataF4.count())
    #




    dataF2 = (dataF.join(airportDf, dataF.src_airport == airportDf.IATA).withColumn("src_airport_name",
        lit(airportDf.Name)). select(col("airlineId"), dataF.Name, col("src_airport_name"),col("row_num"))).join((dataF.join(airportDf,
        dataF.dest_airport == airportDf.IATA).withColumn("dest_airport_name",lit(airportDf.Name)).
        select(col("airlineId"), dataF.Name, col("dest_airport_name"),col("row_num"))),on="row_num").select((dataF.join(airportDf,
        dataF.src_airport == airportDf.IATA).withColumn("src_airport_name",lit(airportDf.Name)). select(col("airlineId"),
        dataF.Name, col("src_airport_name"))).Name,(dataF.join(airportDf, dataF.src_airport == airportDf.IATA).
        withColumn("src_airport_name",lit(airportDf.Name)). select(col("airlineId"), dataF.Name, col("src_airport_name")
        )).airlineId,col("src_airport_name"),col("dest_airport_name"))
    dataF2.show()

    # dataF2 = (dataF.join(airportDf, dataF.src_airport == airportDf.IATA).withColumn("src_airport_name",lit(airportDf.Name))
    #     .select(col("airlineId"), dataF.Name, col("src_airport_name"),col("row_num"))).join((dataF.join(airportDf,
    #     dataF.dest_airport == airportDf.IATA).withColumn("dest_airport_name", lit(airportDf.Name)).select(col("airlineId"),
    #     col("dest_airport_name"),col("row_num"))),on="row_num",how="inner").select(col("Name"),(dataF.join(airportDf,
    #     dataF.src_airport == airportDf.IATA).withColumn("src_airport_name",lit(airportDf.Name)).
    #      select(col("airlineId"),dataF.Name,col("src_airport_name"))).airlineId,col("src_airport_name"),col("dest_airport_name"))

    # dataF2.show()
    # print(dataF2.count())

    dataF = routesNew.join(airlineDf, col("airlineId") == routesNew.airline_id, "inner").filter(routesNew.stops == 0)

    q6 = dataF.join(airportDf, col("src_airport") == airportDf.IATA, "left").select(col("airlineId"),
                                                                                    dataF.Name.alias("airport_name"),
                                                                                    col("dest_airport"),
                                                                                    airportDf.Name.alias("src_airport")). \
        join(airportDf, col("dest_airport") == airportDf.IATA, "left").select(col("airlineId"), col("airport_name"),
                                                                              col("src_airport"),col("Name").alias("destAirport"))

    q6.show()
    print(q6.count())


