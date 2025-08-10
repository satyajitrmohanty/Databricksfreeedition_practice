import dlt
sales_rules={
    "validsales":"sales_id is not null"
}
#empty streaming table
dlt.create_streaming_table(name="sales_stg",expect_all_or_drop=sales_rules)

#creating east sales flow

@dlt.append_flow(target="sales_stg")
def east_sales():
    df=spark.readStream.table("dltsatya.source.sales_east")
    return df

#creating west sales flow

@dlt.append_flow(target="sales_stg")
def west_sales():
    df=spark.readStream.table("dltsatya.source.sales_west")
    return df
