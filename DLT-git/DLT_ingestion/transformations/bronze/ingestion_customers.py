import dlt
from pyspark.sql.functions import col
# Please edit the sample belowduct
#expectations
customers_rules={
    "validcust":"customer_id is not null ",
    "validname":"customer_name is not null"
}

@dlt.table(name="customers_stg")
@dlt.expect_all(customers_rules)
def customers_stg():
    df = spark.readStream \
        .option("skipChangeCommits", "true") \
        .table("dltsatya.source.customers")
    return df


