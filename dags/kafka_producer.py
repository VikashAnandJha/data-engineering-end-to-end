import logging
from confluent_kafka import Producer
import time

# Configure the logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers):
        """
        Initializes the Kafka producer with the given bootstrap servers.
        """
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers
        }
        self.producer = Producer(self.producer_config)

    def produce_message(self, topic, key, value):
        """
        Produces a message to the specified Kafka topic with the given key and value.
        """
        self.producer.produce(topic, key=key, value=value,
                              callback=self.delivery_callback)
        self.producer.flush()

    def delivery_callback(self, err, msg):
        """
        Callback function to handle message delivery status.
        """
        if err:
            logger.error("Message delivery failed: {}".format(err))
        else:
            logger.info("Message delivered to topic [{}], partition [{}], offset [{}]".format(
                msg.topic(), msg.partition(), msg.offset()))
            self.producer.flush()  # Flush after successful delivery


def kafka_producer_main():
    bootstrap_servers = '192.168.135.159:9092'
    kafka_producer = KafkaProducerWrapper(bootstrap_servers)

    topic = "email_topic"
    key = "sample_email@test.com"
    value = "1200000"

    kafka_producer.produce_message(topic, key, value)
    logger.info("Produced message")

    kafka_producer.producer.flush()
    logger.info("Producer flushed.")


if __name__ == "__main__":

    kafka_producer_main()
