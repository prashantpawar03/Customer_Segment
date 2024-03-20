from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_customer_segment.config.ConfigStore import *
from pl_customer_segment.udfs.UDFs import *

def unique_customers_projection(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col
    out0 = in0.select(col("CustomerID"), col("Age"), col("Gender"), col("Location")).distinct()

    return out0
