# from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import *

# create a TableEnvironment

def log_processing():
    # config = Configuration()
    # config.set_string('execution.buffer-timeout', '1 min')
    # env_settings = EnvironmentSettings \
    #     .new_instance() \
    #     .in_streaming_mode() \
    #     .with_configuration(config) \
    #     .build()
    env_settings = EnvironmentSettings.in_streaming_mode()
    
    t_env = TableEnvironment.create(env_settings)
    
    # specify connector and format jars
    t_env.get_config().set("pipeline.jars", "file:///Users/Raghav/Desktop/flink-sql-connector-kafka-1.17.0.jar")
    
    source_ddl = """
            CREATE TABLE source_table_fx(
                timez BIGINT,
                fx_rate DOUBLE
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

    #t_env is an Table Environment - the entry point and central context for creating Table and SQL API programs
    
    # execute_sql() : Executes the given single statement, and return the execution result.
    # Automatically REGISTERS the table source_table_fx (maybe?? else it happens in the from_path method)
    t_env.execute_sql(source_ddl)

    # from_path() : Reads a registered table and returns the resulting Table
    tbl = t_env.from_path('source_table_fx')
    
    # tbl.print_schema()
    # tbl.execute().print()

    
    # sql_query() : Evaluates a SQL query on registered tables and retrieves the result as a Table
    result = t_env.sql_query("SELECT *,\
                               CAST(\
                                CONCAT(\
                                    FROM_UNIXTIME(timez / 1000, 'yyyy-MM-dd HH:mm:ss.'), \
                                    LPAD(CAST(MOD(timez, 1000) AS VARCHAR(3)), 3, '0')) AS TIMESTAMP(3)) AS ltz_time\
                            from %s" % tbl)

    result.print_schema()
    result.execute().print()


if __name__ == '__main__':
    log_processing()