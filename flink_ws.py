from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment

# create a streaming TableEnvironment


def log_processing():
    config = Configuration()
    config.set_string('execution.buffer-timeout', '1 min')
    env_settings = EnvironmentSettings \
        .new_instance() \
        .in_streaming_mode() \
        .with_configuration(config) \
        .build()
    t_env = TableEnvironment.create(env_settings)
    
    # specify connector and format jars
    t_env.get_config().set("pipeline.jars", "file:///Users/Raghav/Desktop/flink-sql-connector-kafka-1.17.0.jar")
    
    source_ddl = """
            CREATE TABLE source_table(
                timez BIGINT,
                price DOUBLE
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
    t_env.execute_sql(source_ddl)

    tbl = t_env.from_path('source_table')
    # tbl.print_schema()

    tbl.execute().print()


if __name__ == '__main__':
    log_processing()