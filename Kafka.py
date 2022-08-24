import json
from typing import Dict
import uuid
import os
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer, DeserializingConsumer, TopicPartition
from kafka3 import KafkaProducer, KafkaConsumer


class KafkaConfluentWriter:
    producer = None
    topic = None

    def __init__(self, topic) -> None:
        load_dotenv()
        self.producer = SerializingProducer({
            'bootstrap.servers': os.getenv('BOOTSTRAPSERVERS'),
            'key.serializer': self.keyencoder,
            'value.serializer': self.valueencoder
        })
        self.topic = topic

    def keyencoder(self, k, ctx) -> str:
        return str.encode(k)

    def valueencoder(self, v, ctx) -> json:
        return json.dumps(v).encode('utf-8')

    def delivery_report(self, err, msg) -> None:
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(
                msg.topic(), msg.partition()))

    def produce(self, key, msg) -> None:
        self.producer.poll(0)
        self.producer.produce(topic=self.topic, key=key,
                              value=msg, on_delivery=self.delivery_report)
        self.producer.flush()


class KafkaConfluentReader:
    consumer = None
    topic = None

    def __init__(self, topic: str, autoCommit: bool) -> None:
        load_dotenv()
        self.topic = topic
        self.consumer = DeserializingConsumer({
            'bootstrap.servers': os.getenv('BOOTSTRAPSERVERS'),
            'group.id': 'buffering',
            'client.id': 'buffering',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': autoCommit,
            'value.deserializer': self.valuedecoder,
            'key.deserializer': self.keydecoder
        })
        self.consumer.assign([TopicPartition(self.topic, 0)])

    def keydecoder(self, k, ctx):
        return k.decode('utf-8')

    def valuedecoder(self, v, ctx):
        return json.loads(v.decode('utf-8'))

    def poll(self, ms) -> object:
        return self.consumer.poll(ms)

    def get_latest_message(self, ms) -> object:
        offsets = self.consumer.get_watermark_offsets(
            TopicPartition(self.topic, 0))
        self.consumer.seek(TopicPartition(
            self.topic, 0, offsets[1]-1 if offsets[1] > 0 else 0))
        return self.consumer.poll(ms)

    def get_all_messages(self, ms) -> {}:
        messages = {}
        while True:
            msg = self.consumer.poll(ms)
            if msg is None:
                break
            else:
                messages[msg.key()] = msg.value()
        return messages

    def close(self) -> None:
        self.consumer.close()
