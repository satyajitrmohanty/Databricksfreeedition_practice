import dlt
from pyspark.sql.functions import*
from pyspark.sql.types import*

#creating mat view

@dlt.table(name="business_sales")
def business_sales():
    df_sales=spark.read.table("fact_sales")
    df_prod=spark.read.table("dim_products")
    df_cust=spark.read.table("dim_customers")

    df_join=(df_sales.join(df_prod,df_sales.product_id==df_prod.product_id,"inner")
        .join(df_cust,df_sales.customer_id==df_cust.customer_id,"inner")
    )

    df_prun=df_join.select("region","category","total_amount")
    df_agg=df_prun.groupBy("region","category").agg(sum("total_amount").alias("total_sales"))

    return df_agg
