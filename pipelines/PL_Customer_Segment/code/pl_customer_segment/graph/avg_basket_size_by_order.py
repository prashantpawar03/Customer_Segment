from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_customer_segment.config.ConfigStore import *
from pl_customer_segment.udfs.UDFs import *

def avg_basket_size_by_order(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import avg
    out0 = in0.groupBy("OrderID").agg(avg("Quantity").alias("avg_basket_size"))

    return out0
