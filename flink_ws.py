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
    
    source_ddl = """
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

    ##### t_env is a Table Environment - the entry point and central context for creating Table and SQL API programs
  
    ##### execute_sql() : Executes the given single statement, and return the execution result (status) -> OK or error.
    ##### Automatically REGISTERS the table 'source_table_fx' (maybe?)
    t_env.execute_sql(source_ddl)

    ##### from_path() : Reads a registered table and returns the resulting Table
    tbl = t_env.from_path('source_table_ws')
    
    print('\nSource Schema (tbl) :')
    tbl.print_schema()
    # tbl.execute().print()

    #####################################################################
    # 2
    # Define Tumbling Window Aggregate Calculation of Revenue per Seller
    #
    # - for every 5 second non-overlapping window
    # - calculate the revenue per seller
    #####################################################################
    
    windowed_rev = (
    tbl.window(Tumble.over(lit(5).seconds).on(col("timez_ltz")).alias("w"))
    .group_by(col("w"))
    .select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("price").avg.alias("avg_price"),
        col("price").count.alias("count")
        )
    )

    print('\nProcess Schema (windowed_rev) :\n')
    windowed_rev.print_schema()
    
    ##### execute() : executes the pipeline and retrieve the transformed data locally during development
    # windowed_rev.execute().print()

    sink_ddl_print = """
        CREATE TABLE printz (
        `window_start` TIMESTAMP(3),
        `window_end` TIMESTAMP(3),
        `avg_price` DOUBLE,
        `count` BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """
    t_env.execute_sql(sink_ddl_print)
      
    windowed_rev.execute_insert('printz').wait()
    
    # statement_set = t_env.create_statement_set()
    # statement_set.add_insert("printx", windowed_rev)
    # statement_set.execute().wait()

if __name__ == '__main__':
    log_processing()