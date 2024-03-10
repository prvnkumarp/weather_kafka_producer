from datetime import datetime

from confluent_kafka import Producer
import logging
logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self, config: dict, kafka_topic: str):
        self._producer = Producer(config)
        self._kafka_topic = kafka_topic
        logger.info("kafka producer initialized successfully")

    @property
    def producer(self):
        return self._producer

    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result.
               Triggered by poll() or flush().
        """
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {str(msg.topic())} at partition [{str(msg.partition())}]')

    def send_msg(self, msg, key=None):
        logger.info("Sending message asynchronously")
        timestamp = int(datetime.now().timestamp() * 1000)
        try:
            self.producer.produce(self._kafka_topic, key=key, value=msg, timestamp=timestamp,
                                  callback=lambda err, original_msg: self.delivery_report(err, original_msg))
            self.producer.poll(0)
        except BufferError as e:
            logger.warning(f"Buffer for {self._kafka_topic} is full. waiting for free space in the queue")
            self.producer.poll(10)
            self.producer.produce(self._kafka_topic, key=key, value=msg,timestamp=timestamp,
                                  callback=lambda err, original_msg: self.delivery_report(err, original_msg))
        finally:
            self.producer.flush()
