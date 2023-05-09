from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import *
from pyflink.table.window import Tumble

#####################################################################
# 1
# Create a TableEnvironment
# Using Table API for manipulation 
# However table environment defined in streaming mode
#####################################################################

def log_processing():

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    
    ##### specify connector and format jars
    t_env.get_config().set("pipeline.jars", "file:///Users/karanbawejapro/Desktop/flinky/flink-sql-connector-kafka-1.17.0.jar")
    t_env.get_config().set("table.exec.source.idle-timeout", "1000")


    # FX Source 
    source_ddl1 = """
        CREATE TABLE source_table_fx(
            timez BIGINT,
            fx_rate DOUBLE,
            timez_ltz AS TO_TIMESTAMP_LTZ(timez,3),
            WATERMARK FOR timez_ltz AS timez_ltz - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'stream2fx',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'fx_group',
            'scan.startup.mode' = 'specific-offsets',
            'scan.startup.specific-offsets' = 'partition:0,offset:0',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'format' = 'json'
        )
        """
    t_env.execute_sql(source_ddl1)
    tbl1 = t_env.from_path('source_table_fx')

     # WS Source 
    source_ddl2 = """
        CREATE TABLE source_table_ws(
            timez BIGINT,
            price DOUBLE,
            timez_ltz AS TO_TIMESTAMP_LTZ(timez,3),
            WATERMARK FOR timez_ltz AS timez_ltz - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'stream1ws',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'ws_group',
            'scan.startup.mode' = 'specific-offsets',
            'scan.startup.specific-offsets' = 'partition:0,offset:0',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'format' = 'json'
        )
        """
    t_env.execute_sql(source_ddl2)
    tbl2 = t_env.from_path('source_table_ws') 

    windowed_fx = (
    tbl1.window(Tumble.over(lit(5).seconds).on(col("timez_ltz")).alias("w"))
    .group_by(col("w"))
    .select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("fx_rate").avg.alias("avg_fx_rate"),
        col("fx_rate").count.alias("count")
        )
    )

    windowed_ws = (
    tbl2.window(Tumble.over(lit(5).seconds).on(col("timez_ltz")).alias("w"))
    .group_by(col("w"))
    .select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("price").avg.alias("avg_price"),
        col("price").count.alias("count")
        )
    )

    sink_ddl1_print = """
        CREATE TABLE printx (
        `window_start` TIMESTAMP(3),
        `window_end` TIMESTAMP(3),
        `avg_fx_rate` DOUBLE,
        `count` BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """
    t_env.execute_sql(sink_ddl1_print)

    sink_ddl2_print = """
        CREATE TABLE printz (
        `window_start` TIMESTAMP(3),
        `window_end` TIMESTAMP(3),
        `avg_price` DOUBLE,
        `count` BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """
    t_env.execute_sql(sink_ddl2_print)

    
    # statement_set1 = t_env.create_statement_set()
    # statement_set1.add_insert("printx", windowed_fx)
    # statement_set1.execute()

    # statement_set2 = t_env.create_statement_set()
    # statement_set2.add_insert("printz", windowed_ws)
    # statement_set2.execute()

if __name__ == '__main__':
    log_processing()




    # # ADAM MQ-> Use Table API to perform inner join between sellers and product sales
    # # to yield result of all sellers who have sales
    # seller_products = sales_tbl.join(sellers_tbl, sales_tbl.seller_id == sellers_tbl.id)\
    #                         .select(sellers_tbl.city, sellers_tbl.state,
    #                               sales_tbl.product, sales_tbl.product_price)\
    #                         .distinct()

    # print('\nseller_products data')
    # print(seller_products.to_pandas())
