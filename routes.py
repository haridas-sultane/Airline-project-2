from pyspark.sql import SparkSession,Row
if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("project").getOrCreate()
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    from pyspark.sql.window import *


    newrouteDf = spark.read.csv(r"C:\Users\harid\PycharmProjects\pythonProject1\converted\routes air", header=True)
    newrouteDf.show()
    # print(newrouteDf.count())
    newrouteDf2 = newrouteDf.withColumn("airline_id", (col("airline_id")).cast(IntegerType())).withColumn(
        "src_airport_id", (col("src_airport_id")).cast(IntegerType())). \
        withColumn("dest_airport_id", (col("dest_airport_id")).cast(IntegerType()))

    airlineDf = spark.read.csv(r"D:\pyspark file\airline data\airline.csv", header=True,
                               schema="airlineId integer,Name string,Alias string,IATA string,ICAO string,Callasign string,Country string,Active string")
    airlineDf.show()
    airlineDf.createOrReplaceTempView("airline")

    airportDf=spark.read.csv(r"D:\pyspark file\airline data\airport.csv",schema="Airport_ID integer,Name string,City string,Country string,IATA string,ICAO string,Latitude double,Longitude double,Altitude long,Timezone string,DST string,Tz string,Type string,Source string")
    airportDf.show()
    airportDf.createOrReplaceTempView("airport")


    newrouteDf2 =newrouteDf2.na.fill(-1).na.fill("(unknown)")
    newrouteDf2.show()
    newrouteDf2.createOrReplaceTempView("route")

    # que4 = spark.sql(
    #     "select * from airport c where c.iata in (select b.src_airport from(select a.src_airport, sum(takeoff),rank()\
    #      over (order by sum(takeoff) ) as rankNum from (select src_airport,count(*) as takeoff  from route group by\
    #       src_airport union select dest_airport,count(*) from route group by dest_airport) a group by a.src_airport)\
    #        b where rankNum=1) ").

    ######     OR ###############

    # que4a = spark.sql("select * from airport f where f.iata in (select e.src_airport from (select rank() over \
    #                     (order by d.total asc) as rankDsc,d.src_airport from( select c.src_airport, (c.takeoff + c.landing) \
    #                      as total from (select src_airport, takeoff, \
    #                     landing from (select distinct(src_airport), count(*) as takeoff from route group by src_airport) \
    #                     a join (select distinct(dest_airport), count(*) as landing from route group by dest_airport) b on \
    #                     a.src_airport=b.dest_airport) c  order by total desc ) d)e where e.rankDsc=1)"
    #                   )
    #
    # que4a.show()
print("--------------------------------------------------------------")
    # que5 = spark.sql(
    #     " select * from airport c where c.iata = (select b.src_airport from(select a.src_airport, sum(takeoff),row_number()\
    #      over (order by sum(takeoff) desc) as rowNum from (select src_airport,count(*) as takeoff  from route group by\
    #       src_airport union select dest_airport,count(*) from route group by dest_airport) a group by a.src_airport)\
    #        b where rowNum=1) ")
    # que5.show()
                    #######     OR     ########
    # que5a=spark.sql("select * from airport f where f.iata=(select e.src_airport from (select rank() over \
    #                 (order by d.total desc) as rankDsc,d.src_airport from( select c.src_airport, (c.takeoff + c.landing) \
    #                  as total from (select src_airport, takeoff, \
    #                 landing from (select distinct(src_airport), count(*) as takeoff from route group by src_airport) \
    #                 a join (select distinct(dest_airport), count(*) as landing from route group by dest_airport) b on \
    #                 a.src_airport=b.dest_airport) c  order by total desc ) d)e where e.rankDsc=1)"
    #                 )
    #
    # que5a.show()

print("______________________________________________________________")

    # que2=spark.sql("select distinct( a.Country ) from airport a join airline b on a.Country=b.Country ")
    # que2.show()

print("_______________________________________________________________")
    # que3=spark.sql("select a.Name,b.src_airport,b.airline_id,count(*) from airline a join route b\
    #                     on a.airlineId = b.airline_id group by a.Name,b.src_airport,b.airline_id having count(*)>3")
    # que3.show()

