# import dlt
# #creating end to end basic pipeline

# @dlt.table(name="staging_orders")
# def staging_orders():
#     df=spark.readStream.table("dltsatya.source.orders")
#     return df


# from pyspark.sql.functions import*

# @dlt.view(name="transformed_orders")
# def transformed_orders():
#     df=spark.readStream.table("staging_orders")
#     df=df.withColumn("order_status",upper(col("order_status")))
#     return df

# @dlt.table(name="aggregated_orders")
# def aggregated_orders():
#     df=spark.readStream.table("transformed_orders")
#     df=df.groupBy("order_status").count()
#     return df
# # DBTITLE 1)

