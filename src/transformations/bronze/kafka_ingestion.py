from pyspark import pipelines as dp

API_KEY = dbutils.secrets.get(scope="kafka_credentials", key="API_KEY")
API_SECRET = dbutils.secrets.get(scope="kafka_credentials", key="API_SECRET")

@dp.table(
    comment="Bronze table ingesting data from Kafka"
)
def bronze_table():
    return spark.readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", 'pkc-921jm.us-east-2.aws.confluent.cloud:9092') \
        .option("subscribe", "my_first_topic") \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{API_KEY}' password='{API_SECRET}';") \
        .load()
