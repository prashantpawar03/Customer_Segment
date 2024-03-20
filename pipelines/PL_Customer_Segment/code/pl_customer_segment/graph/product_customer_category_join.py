from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_customer_segment.config.ConfigStore import *
from pl_customer_segment.udfs.UDFs import *

def product_customer_category_join(
        spark: SparkSession, 
        in0: DataFrame, 
        in1: DataFrame, 
        in2: DataFrame, 
        in3: DataFrame
) -> DataFrame:
    out0 = in0\
               .join(in2, in0.ProductID == in2.ProductID, "inner")\
               .join(in3, in0.Category == in3.Category, "inner")\
               .join(in1, in2.CustomerID == in1.CustomerID, "inner")\
               .select(in2.OrderID.cast("int"), in1.CustomerID.cast("int"), in1.Age.cast("int"), in1.Gender, in1.Location, in0.ProductID, in0.Category, in0.Brand, in0.Type, in2.Quantity.cast("int"), in2.Price.cast("int"), in3.Description)

    return out0
