# import dlt
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# sources={
#     "eastloc":"sales_east",
#     "westloc":"sales_west"   
# }

# sales_rules={
#     "validsales":"sales_id is not null"
# }

# dlt.create_streaming_table(name="sales_stg_dynamic",expect_all_or_drop=sales_rules)

# #write function which will read from multiple sources
# def read_sales_sources(loc,path):
#     @dlt.append_flow(target="sales_stg_dynamic",name=f"{loc}_flow")
#     def read_sales():
#         df=spark.readStream.table(f"dltsatya.source.{path}")
#         return df

# #iterate for each region and call function
# for loc,path in sources.items():
#     read_sales_sources(loc,path)
