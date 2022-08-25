import json
import os
import uuid
from typing import Dict
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer, DeserializingConsumer, TopicPartition

# custom class based on confluent_kafka SerializigProducer capabilities‚
class KafkaConfluentWriter:
    producer = None
    topic = None

    # topic can be set when instanziating the writer class
    def __init__(self, topic: str) -> None:
        load_dotenv()  # required to read env variables
        self.producer = SerializingProducer({
            # servers from env variable to switch more easily based on used environment
            'bootstrap.servers': os.getenv('BOOTSTRAPSERVERS'),
            'key.serializer': self.keyencoder,
            'value.serializer': self.valueencoder
        })
        self.topic = topic

    # helper function for serialization of key
    def keyencoder(self, k: str, ctx) -> str:
        return str.encode(k)

    # helper function for serialization of value
    def valueencoder(self, v: str, ctx) -> json:
        return json.dumps(v).encode('utf-8')

    # Called once for each message produced to indicate delivery result. Triggered by poll() or flush()
    def delivery_report(self, err, msg) -> None:
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(
                msg.topic(), msg.partition()))

    # store new message in topic
    def produce(self, key, msg) -> None:
        self.producer.poll(0)
        self.producer.produce(topic=self.topic, key=key,
                              value=msg, on_delivery=self.delivery_report)
        self.producer.flush()

# custom class based on confluent_kafka DeserializingConsumer capabilities
class KafkaConfluentReader:
    consumer = None
    topic = None

    # topic and commit strategy (auto commits) can be set when instanziating the writer class
    def __init__(self, topic: str, autoCommit: bool) -> None:
        load_dotenv()  # required to read env variables
        self.topic = topic
        self.consumer = DeserializingConsumer({
            # servers from env variable to switch more easily based on used environment
            'bootstrap.servers': os.getenv('BOOTSTRAPSERVERS'),
            'group.id': uuid.uuid4().hex,  # using some unique id for the reader
            'client.id': uuid.uuid4().hex,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': autoCommit,
            'value.deserializer': self.valuedecoder,
            'key.deserializer': self.keydecoder
        })
        # assign consumer to provided topic and first partition
        # limitation: additional logic would be needed when working with multiple partitions
        self.consumer.assign([TopicPartition(self.topic, 0)])

    # helper function for deserialization of key
    def keydecoder(self, k: str, ctx):
        return k.decode('utf-8')

    # helper function for deserialization of value
    def valuedecoder(self, v: json, ctx):
        return json.loads(v.decode('utf-8'))

    # poll messages
    def poll(self, ms: float) -> object:
        return self.consumer.poll(ms)

    # get the most recent message, no matter where the actual offset would be
    def get_latest_message(self, ms: float) -> object:
        offsets = self.consumer.get_watermark_offsets(
            TopicPartition(self.topic, 0))  # get highest possible offset
        self.consumer.seek(TopicPartition(
            self.topic, 0, offsets[1]-1 if offsets[1] > 0 else 0))  # seek to highest offset minus one message
        return self.consumer.poll(ms)  # poll message

    # get list of unconsumed messages
    def get_all_messages(self, ms: float) -> Dict[str, str]:
        messages = {}  # messages as dictionary
        offsets = self.consumer.get_watermark_offsets(
            TopicPartition(self.topic, 0))  # get smallest possible offset
        self.consumer.seek(TopicPartition(
            self.topic, 0, offsets[0]))  # seek to beginning
        while True:
            msg = self.consumer.poll(ms)
            if msg is None:
                break  # stop and return list if no new messages are left
            else:
                messages[msg.key()] = msg.value()
        return messages

    # close consumer
    def close(self) -> None:
        self.consumer.close()
