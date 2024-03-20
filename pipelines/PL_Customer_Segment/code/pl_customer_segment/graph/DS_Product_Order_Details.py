from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_customer_segment.config.ConfigStore import *
from pl_customer_segment.udfs.UDFs import *

def DS_Product_Order_Details(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("OrderID", StringType(), True), StructField("CustomerID", StringType(), True), StructField("ProductID", StringType(), True), StructField("Quantity", StringType(), True), StructField("Price", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/Prashant_src/Product_Order_Data.txt")
