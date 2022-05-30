from pyspark.sql import SparkSession,Row
if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("project").getOrCreate()
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    from pyspark.sql.window import *


    # def replace(info):
    #     if info is StringType():
    #         if info is None:
    #             return "(unknown)"
    #         else:
    #             return info
    #     elif info is IntegerType():
    #         if info is None:
    #             return -1
    #         else:
    #             return info
    #     else:
    #         return info

    def replace(info):
        if info is None:
            return "(unknown)"
        else:
            return info

    createUdf = udf(lambda x: replace(x),StringType())

    newrouteDf = spark.read.csv(r"C:\Users\harid\PycharmProjects\pythonProject1\converted\routes air", header=True)
    # newrouteDf.show()
    # print(newrouteDf.count())
    newrouteDf2 = newrouteDf.withColumn("airline_id", (col("airline_id")).cast(IntegerType())).withColumn(
        "src_airport_id", (col("src_airport_id")).cast(IntegerType())). \
        withColumn("dest_airport_id", (col("dest_airport_id")).cast(IntegerType()))

    newrouteDf2.withColumn("codeshare",createUdf(newrouteDf2.codeshare))\
        .withColumn("airline_id",createUdf(newrouteDf2.airline_id)).show(500)