#
# COUNT NUMBER OF FIRSTNAME ORDER BY count desc
#

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.window import Window

import time
import sys

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()        
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % \
                (method.__name__, (te - ts) * 1000))
        return result    
    return timed

@timeit
def evolutionByFirstName(firstName):

    lines = spark.read\
             .option("header",True)\
             .option("delimiter", ";")\
             .csv("./data/dpt2018.csv")

    window = Window.orderBy("year")

    lines.filter(sf.col("preusuel") == firstName.upper())\
            .filter(sf.col("annais") != "XXXX")\
            .groupBy("annais").agg(sf.sum("nombre").alias("nombre"))\
            .withColumnRenamed("annais","year")\
            .withColumn("nb_year_1", sf.lag("nombre", 1, 0).over(window))\
            .withColumn("% evolution", sf.when(sf.col("nb_year_1") != 0,(sf.col("nombre")-sf.col("nb_year_1"))/sf.col("nb_year_1")*100).otherwise(0))\
            .show(120,False)


if __name__ == "__main__":

    spark = SparkSession\
            .builder\
            .master("local[*]")\
            .appName("Count")\
            .getOrCreate()

    if len(sys.argv)>1:
        evolutionByFirstName(sys.argv[1])
    else:
        print("You must set argument firstname")

    spark.stop()

