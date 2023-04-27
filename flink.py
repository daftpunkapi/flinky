from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment

# create a streaming TableEnvironment
config = Configuration()
config.set_string('execution.buffer-timeout', '1 min')
env_settings = EnvironmentSettings \
    .new_instance() \
    .in_streaming_mode() \
    .with_configuration(config) \
    .build()

table_env = TableEnvironment.create(env_settings)