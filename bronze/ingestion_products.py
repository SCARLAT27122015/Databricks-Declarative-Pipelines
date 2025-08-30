import dlt

#products expectations
products_rules = {
    'rule_1': 'product_id IS NOT NULL'
    , 'rule_2': 'price >=0'
}


#ingesting products


@dlt.table
@dlt.expect_all_or_drop(products_rules)
def products_stg():
    df = spark.readStream.table('delta_live.source.products')

    return df
