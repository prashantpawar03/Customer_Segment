from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_customer_segment.config.ConfigStore import *
from pl_customer_segment.udfs.UDFs import *

def DS_Items_Per_Order(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", True)\
        .option("sep", ",")\
        .mode("error")\
        .option("separator", ",")\
        .option("header", True)\
        .csv("dbfs:/FileStore/Prashant_src/Output_Items_per_Order")
