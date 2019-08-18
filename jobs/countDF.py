#
# COUNT NUMBER OF FIRSTNAME ORDER BY count desc
#

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

import time

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
def countDF():

    lines = spark.read\
             .option("header",True)\
             .option("delimiter", ";")\
             .csv("./data/dpt2018.csv")

    lines.groupBy("preusuel").agg(sf.sum("nombre").alias("nombre")).orderBy(sf.desc("nombre")).show()


if __name__ == "__main__":

    spark = SparkSession\
            .builder\
            .master("local[*]")\
            .appName("Count")\
            .getOrCreate()

    countDF()

    spark.stop()

