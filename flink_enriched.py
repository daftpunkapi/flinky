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
        col("w").start.alias("window_start_fx"),
        col("w").end.alias("window_end_fx"),
        col("fx_rate").avg.alias("avg_fx_rate"),
        col("fx_rate").count.alias("count")
        )
    )

    windowed_ws = (
    tbl2.window(Tumble.over(lit(5).seconds).on(col("timez_ltz")).alias("w"))
    .group_by(col("w"))
    .select(
        col("w").start.alias("window_start_ws"),
        col("w").end.alias("window_end_ws"),
        col("price").avg.alias("avg_price"),
        col("price").count.alias("count")
        )
    )

    print("helloworld1")

    sink_ddl1_print = """
        CREATE TABLE printx (
        `window_start_fx` TIMESTAMP(3),
        `window_end_fx` TIMESTAMP(3),
        `avg_fx_rate` DOUBLE,
        `count` BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """
    t_env.execute_sql(sink_ddl1_print)

    sink_ddl2_print = """
        CREATE TABLE printz (
        `window_start_ws` TIMESTAMP(3),
        `window_end_ws` TIMESTAMP(3),
        `avg_price` DOUBLE,
        `count` BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """
    t_env.execute_sql(sink_ddl2_print)

    print("helloworld2")

    # Join the two tables on the window start and end time
    joined_tables = (
        windowed_fx.join(windowed_ws)
        .where(col("window_start_ws") == col("window_start_fx"))
        .where(col("window_end_ws") == col("window_end_fx"))
        .select(
            col("window_start_ws"),
            col("window_end_ws"),
            col("avg_fx_rate"),
            col("avg_price")
            )
        )
    
    print("helloworld3")
   
    # Calculate the product of avg_fx_rate and avg_price
    result_table = joined_tables.select(
        col("window_start_ws"),
        col("window_end_ws"),
        col("avg_fx_rate") * col("avg_price")).alias("result")
    
    print("helloworld4")

    # Define the Kafka sink
    sink_ddl_kafka = """
        CREATE TABLE sink_kafka (
            window_start_ws TIMESTAMP(3),
            window_end_ws TIMESTAMP(3),
            result DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'output',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    print("helloworld5")
    
    # Execute the Kafka sink DDL
    t_env.execute_sql(sink_ddl_kafka)

    # Write the result table to the Kafka sink
    result_table.execute_insert("sink_kafka")


if __name__ == '__main__':
    log_processing()


    # statement_set1 = t_env.create_statement_set()
    # statement_set1.add_insert("printx", windowed_fx)
    # statement_set1.execute()

    # statement_set2 = t_env.create_statement_set()
    # statement_set2.add_insert("printz", windowed_ws)
    # statement_set2.execute()


    # # ADAM MQ-> Use Table API to perform inner join between sellers and product sales
    # # to yield result of all sellers who have sales
    # seller_products = sales_tbl.join(sellers_tbl, sales_tbl.seller_id == sellers_tbl.id)\
    #                         .select(sellers_tbl.city, sellers_tbl.state,
    #                               sales_tbl.product, sales_tbl.product_price)\
    #                         .distinct()

    # print('\nseller_products data')
    # print(seller_products.to_pandas())
