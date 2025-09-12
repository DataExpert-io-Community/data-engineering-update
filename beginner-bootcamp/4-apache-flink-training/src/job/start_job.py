from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import ScalarFunction, udf
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
import os
import json
import requests


def create_processed_events_sink_kafka(t_env):
    table_name = "raw_events_kafka"
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    sasl_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_GROUP').split('.')[0] + '.' + table_name}',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.jaas.config' = '{sasl_config}',
            'format' = 'json'
        );
        """
    print(sink_ddl)
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

class GetLocation(ScalarFunction):
  def eval(self, ip_address):
    url = "https://api.ip2location.io"
    response = requests.get(url, params={
        'ip': ip_address,
        'key': os.environ.get("IP_CODING_KEY")
    })

    if response.status_code != 200:
        # Return empty dict if request failed
        return json.dumps({})

    data = json.loads(response.text)

    # Extract the country and state from the response
    # This might change depending on the actual response structure
    country = data.get('country_code', '')
    state = data.get('region_name', '')
    city = data.get('city_name', '')
    return json.dumps({'country': country, 'state': state, 'city': city})

get_location = udf(GetLocation(), result_type=DataTypes.STRING())



def create_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}')
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    print(source_ddl)
    t_env.execute_sql(source_ddl)
    return table_name


def log_processing():
    print('Starting Sessionization Job!')

    # --- Execution Environment ---
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # checkpoint every 10 seconds
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Register UDF for geo lookup
    t_env.create_temporary_function("get_location", get_location)

    try:
        # --- Source and Sink Tables ---
        source_table = create_events_source_kafka(t_env)

        # Adjust sink schema to hold session aggregates
        postgres_sink = "processed_sessions"
        sink_ddl = f"""
            CREATE TABLE {postgres_sink} (
                ip VARCHAR,
                host VARCHAR,
                session_start TIMESTAMP(3),
                session_end TIMESTAMP(3),
                num_events BIGINT,
                geodata VARCHAR
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{os.environ.get("POSTGRES_URL")}',
                'table-name' = '{postgres_sink}',
                'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
                'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
                'driver' = 'org.postgresql.Driver'
            );
        """
        t_env.execute_sql(sink_ddl)

        print('Running session window aggregation...')

        # --- Sessionization by IP + Host ---
        (
            t_env.from_path(source_table)
                .window(Session.with_gap(lit(5).minutes).on(col("event_timestamp")).alias("w"))
                .group_by(col("w"), col("ip"), col("host"))
                .select(
                    col("ip"),
                    col("host"),
                    col("w").start.alias("session_start"),
                    col("w").end.alias("session_end"),
                    col("*").count.alias("num_events"),
                    get_location(col("ip")).alias("geodata")
                )
                .execute_insert(postgres_sink)
                .wait()
        )

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))



if __name__ == '__main__':
    log_processing()
