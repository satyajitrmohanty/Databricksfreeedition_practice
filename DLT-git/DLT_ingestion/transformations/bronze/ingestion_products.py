import dlt
from pyspark.sql.functions import col
# Please edit the sample below
products_rules={
    "validproduct":"product_id is not null ",
    "validprice":"price >= 0"
}

@dlt.table(name="products_stg")
@dlt.expect_all_or_drop(products_rules)

def products_stg():
    df=spark.readStream.table("dltsatya.source.products")
    return df
