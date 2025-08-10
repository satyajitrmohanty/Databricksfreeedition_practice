# #creating streaming table

# import dlt

# @dlt.table(
#     name="first_stream_table"
#     )
# def first_stream_table():
#     df=(
#         spark.readStream.table("dltsatya.source.orders")
#         )
#     return df
    
# #materialized views for batch

# @dlt.table(name="first_mat_view")
# def first_mat_view():
#     df=spark.read.table("dltsatya.source.orders")
#     return df

# #create batch views
# @dlt.view(name="first_batch_view")
# def first_batch_view():
#     df=spark.read.table("dltsatya.source.orders")
#     return df

# #create streaming views
# @dlt.view(name="first_streaming_view")
# def first_streaming_view():
#     df=spark.readStream.table("dltsatya.source.orders")
#     return df

