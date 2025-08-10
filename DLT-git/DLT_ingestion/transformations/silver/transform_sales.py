import dlt
from pyspark.sql.functions import*
from pyspark.sql.types import*

@dlt.view(name="sales_transformed_view")
def sales_transformed_view():
    df=spark.readStream.table("sales_stg")
    df=df.withColumn("total_amount", col("quantity")*col("amount"))
    return df

dlt.create_streaming_table(
  name = "transformed_sales")

dlt.create_auto_cdc_flow(
  target = "transformed_sales",
  source = "sales_transformed_view",
  keys = ["sales_id"],
  sequence_by = "sale_timestamp",
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

