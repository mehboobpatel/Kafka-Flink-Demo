import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # 1. Create environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    tbl_env = StreamTableEnvironment.create(env, environment_settings=settings)
    #//tbl_env is like sessoin in spark

    # 2. Resolve absolute JAR paths
    base_dir = os.path.abspath(os.path.dirname(__file__))

    kafka_jar_path = os.path.join(base_dir, "flink-sql-connector-kafka_2.11-1.13.0.jar")
    json_jar_path = os.path.join(base_dir, "flink-json-1.13.0.jar")

    # 3. Set both jars using pipeline.jars config
    tbl_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        f"file:///{kafka_jar_path.replace(os.sep, '/')};file:///{json_jar_path.replace(os.sep, '/')}"
    )

    # 4. Kafka Source Table
    tbl_env.execute_sql("""
        CREATE TABLE sales_inr (
            seller_id STRING,
            amount_inr DOUBLE,
            sale_ts BIGINT,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-inr',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'sales-group',
            'format' = 'json'
        )
    """)
    tbl = tbl_env.from_path('sales_inr')
    print('\nSource Table Schema')
    tbl.print_schema()


    # 5. Query Logic (INR to EUR tumbling window)
    result = tbl_env.sql_query("""
        SELECT
            seller_id,
            TUMBLE_END(proctime, INTERVAL '60' SECOND) AS window_end,
            SUM(amount_inr) * 0.010 AS window_sales
        FROM sales_inr
        GROUP BY TUMBLE(proctime, INTERVAL '60' SECOND), seller_id
    """)


        # 6. Kafka Sink Table
    tbl_env.execute_sql("""
        CREATE TABLE sales_euros (
            seller_id STRING,
            window_end TIMESTAMP(3),
            window_sales DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-euros',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """)


    print('\nProcess Sink Schema')
    result.print_schema()

    # 7. Sink the result
    result.execute_insert("sales_euros").wait()

    # 8. Optional job name
    tbl_env.execute("INR to Euro Conversion Job")

if __name__ == "__main__":
    main()
