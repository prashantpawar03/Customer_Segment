from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_customer_segment.config.ConfigStore import *
from pl_customer_segment.udfs.UDFs import *

def order_frequency_by_customer(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import count
    out0 = in0.groupBy("CustomerID").count().withColumnRenamed("count", "Frequency")

    return out0
