"""Data destinations"""
from __future__ import annotations

import abc

from datacat.config import Configuration
from datacat.typing import RawRow

# TODO(alvaro): Maybe serialization should be tied to the Sink?


def build(conf: Configuration) -> Sink:
    """Build the right `Sink` for the given configuration"""

    if conf.sink.type == "console":
        return ConsoleSink()
    if conf.sink.type == "kafka":
        return KafkaSink(
            bootstrap_servers=conf.sink.bootstrap_servers, topic=conf.sink.topic
        )
    raise ValueError("Unknown sink configuration")


class Sink(abc.ABC):
    """An object that outputs datasets into some format"""

    @abc.abstractmethod
    async def output(self, row: RawRow):
        ...

    async def init(self):
        pass

    async def teardown(self):
        pass


class ConsoleSink(Sink):
    """A sink that outputs the rows to the console"""

    async def output(self, row: RawRow):
        print(row)


class KafkaSink(Sink):
    """A sink that outputs the rows to a Kafka Topic"""

    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

    async def output(self, row: RawRow):
        assert self.producer is not None
        await self.producer.send_and_wait(self.topic, row.encode())

    async def init(self):
        import aiokafka

        self.producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers, enable_idempotence=True
        )
        await self.producer.start()

    async def teardown(self):
        await self.producer.stop()
