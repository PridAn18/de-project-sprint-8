
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType
from kafka import KafkaProducer

TOPIC_NAME_IN = 'a.navaro_in'
TOPIC_NAME_OUT = 'a.navaro_out'

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    if not df.isEmpty():
        # Добавляем поле feedback (логика получения фидбэка может быть изменена)
        df_with_feedback = df.withColumn("feedback", f.lit(None))  # Замените None на вашу логику получения фидбэка

        # Записываем df в PostgreSQL с полем feedback
        jdbc_url = "jdbc:postgresql://localhost:5432/de"
        properties = {
            "user": "jovyan",
            "password": "jovyan",
            "driver": "org.postgresql.Driver"
        }

        # Запись в таблицу subscribers_feedback
        df_with_feedback.write.jdbc(url=jdbc_url, table="public.subscribers_feedback", mode="append", properties=properties)

        # Создаем df для отправки в Kafka без поля feedback
        kafka_df_to_send = df.select([col for col in df.columns if col != "feedback"])
        
        # Сериализация в JSON с помощью to_json()
        kafka_messages = kafka_df_to_send.select(f.to_json(f.struct("*")).alias("value"))

        # Подготовка для отправки в Kafka
        kafka_messages_list = kafka_messages.collect()

        # Отправляем сообщения в результирующий топик Kafka
        producer = KafkaProducer(bootstrap_servers='rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
                                 value_serializer=lambda v: v[0].encode('utf-8'))
        
        for message in kafka_messages_list:
            producer.send(TOPIC_NAME_OUT, value=message.value)

        producer.flush()
        # Очищаем память от DataFrame
        df.unpersist()
                        

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
    'kafka.ssl.truststore.location': '/usr/local/share/ca-certificates/Yandex/YandexCA.crt' 
}

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), nullable=False),  # UUID ресторана
    StructField("adv_campaign_id", StringType(), nullable=False),  # UUID рекламной кампании
    StructField("adv_campaign_content", StringType(), nullable=True),  # текст кампании
    StructField("adv_campaign_owner", StringType(), nullable=True),  # владелец кампании
    StructField("adv_campaign_owner_contact", StringType(), nullable=True),  # контакт владельца
    StructField("adv_campaign_datetime_start", LongType(), nullable=False),  # время начала кампании
    StructField("adv_campaign_datetime_end", LongType(), nullable=False),  # время окончания кампании
    StructField("datetime_created", LongType(), nullable=False)  # время создания кампании
])

# читаем и десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (spark.readStream.format('kafka')
          .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
          .option("subscribe", TOPIC_NAME_IN)
          .options(**kafka_security_options)
          .option("maxOffsetsPerTrigger", 1000)
          .load()
          .withColumn('value', f.col('value').cast(StringType()))
          .withColumn('event', f.from_json(f.col('value'), incomming_message_schema))
          .selectExpr('event.*')
          .withColumn('trigger_datetime_created', f.round(f.unix_timestamp(f.current_timestamp())))
          .filter((col("adv_campaign_datetime_start") <= col("trigger_datetime_created")) & (col("adv_campaign_datetime_end") >= col('trigger_datetime_created')))
          )

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                      .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'student') \
                    .option('password', 'de-student') \
                    .load()

subscribers_restaurant_df = subscribers_restaurant_df.withColumnRenamed("restaurant_id", "subscriber_restaurant_id") # для избежания коллизий

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = (filtered_read_stream_df.join(subscribers_restaurant_df, filtered_read_stream_df.restaurant_id == subscribers_restaurant_df.subscriber_restaurant_id)
             .withColumn('client_id', subscribers_restaurant_df.client_id)
             .select('restaurant_id','adv_campaign_id','adv_campaign_content','adv_campaign_owner','adv_campaign_owner_contact','adv_campaign_datetime_start','adv_campaign_datetime_end','client_id', 'datetime_created','trigger_datetime_created')
             )
# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination() 


