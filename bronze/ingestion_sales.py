import dlt

#sales expectations
sales_rules = {
    'rule_1': 'sales_id IS NOT NULL'
}


#1 Let us create an empty streaming table
dlt.create_streaming_table(
    name = 'sales_stg'
    , expect_all_or_drop=sales_rules
)


#Creating East sales flow based on east sales table
@dlt.append_flow(target='sales_stg')
def east_sales():
    df = spark.readStream.table('delta_live.source.sales_east')
    return df

#Creating west sales flow based on west sales table
@dlt.append_flow(target='sales_stg')
def west_sales():
    df = spark.readStream.table('delta_live.source.sales_west')
    return df
