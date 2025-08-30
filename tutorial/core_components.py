#====================CORE CONCEPTS OF DECLARATIVE PIPELINES (DELTA LIVE TABLES)=======================================#

#How to create a streaming table

import dlt

@dlt.table
def first_stream_table():
    df = (spark.readStream
                .table('delta_live.source.orders')
    )

    return df


#How to create Materialized view (batch mode of data)
@dlt.table
def first_mat_view():
    df = spark.read.table('delta_live.source.orders')
    return df


#How to create a batch view
@dlt.view
def first_batch_view():
    df = spark.read.table('delta_live.source.orders')
    return df


#How to create a stream view
@dlt.view
def first_stream_view():
    df = spark.readStream.table('delta_live.source.orders')
    return df
