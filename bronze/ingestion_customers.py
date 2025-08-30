import dlt

#customers expectations
customers_rules = {
    'rule_1': 'customer_id IS NOT NULL'
    , 'rule_2': 'customer_name IS NOT NULL'
}


#ingesting customers


@dlt.table
@dlt.expect_all_or_drop(customers_rules)
def customers_stg():
    df = spark.readStream.table('delta_live.source.customers')

    return df
