import dlt
from pyspark.sql.functions import*
from pyspark.sql.types import*

@dlt.view(name="customers_transformed_view")
def customers_transformed_view():
    df=spark.readStream.table("customers_stg")\
      .withColumn("customer_name",upper(col("customer_name")))
    return df

#creating destination silver table
dlt.create_streaming_table(
  name = "transformed_customers")
  
dlt.create_auto_cdc_flow(
  target = "transformed_customers",
  source = "customers_transformed_view",
  keys = ["customer_id"],
  sequence_by = "last_updated",
  stored_as_scd_type = 1,
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = False
)

