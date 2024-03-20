from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pl_customer_segment.config.ConfigStore import *
from pl_customer_segment.udfs.UDFs import *
from prophecy.utils import *
from pl_customer_segment.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_Customer_Data = DS_Customer_Data(spark)
    df_DS_Category_description = DS_Category_description(spark)
    df_DS_Product_Details = DS_Product_Details(spark)
    df_DS_Product_Order_Details = DS_Product_Order_Details(spark)
    df_Product_Category_Description = Product_Category_Description(spark, df_DS_Category_description)
    df_dataset_joins = dataset_joins(
        spark, 
        df_DS_Product_Details, 
        df_DS_Customer_Data, 
        df_DS_Product_Order_Details, 
        df_Product_Category_Description
    )
    df_avg_basket_size_by_order = avg_basket_size_by_order(spark, df_dataset_joins)
    df_unique_customers_projection = unique_customers_projection(spark, df_dataset_joins)
    DS_Customer_demographics(spark, df_unique_customers_projection)
    df_order_frequency_by_customer = order_frequency_by_customer(spark, df_dataset_joins)
    DS_Purchase_Frequency(spark, df_order_frequency_by_customer)
    df_total_purchase_by_customer = total_purchase_by_customer(spark, df_dataset_joins)
    DS_Items_Per_Order(spark, df_avg_basket_size_by_order)
    DS_Total_Purchase_by_Customer(spark, df_total_purchase_by_customer)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PL_Customer_Segment")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/PL_Customer_Segment", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/PL_Customer_Segment")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
