import dlt

#create empty streaming table

dlt.create_streaming_table(name="dim_products")
#auto cdc flow using the transformer view from the silver

dlt.create_auto_cdc_flow(
  target = "dim_products",
  source = "products_transformed_view",
  keys = ["product_id"],
  sequence_by = "last_updated",
  stored_as_scd_type = 2,
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
