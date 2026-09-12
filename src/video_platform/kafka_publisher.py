"""Publish simulated frames to Kafka.

Partitioning strategy
----------------------
Each message is keyed by ``stream_id`` (UTF-8 encoded). Kafka's default
partitioner hashes the message key to choose a partition, so every message
for a given stream is always routed to the *same* partition. Kafka only
guarantees ordering within a partition, so keying by ``stream_id`` is what
gives per-stream ordering: consumers reading a single partition see that
stream's frames in the same sequence they were produced, even though frames
from different streams may interleave across partitions and be processed out
of order relative to each other.

The producer in this module talks to a real Kafka broker (see
``docker/docker-compose.yml`` for a local single-broker instance for
development/testing) through the optional ``kafka-python`` dependency. Tests
exercise the serialization and key-selection logic against an in-memory fake
producer, so they need neither a broker nor the real client library.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Protocol

from .simulator import Frame

DEFAULT_TOPIC = "video-frames"


class KafkaProducerLike(Protocol):
    """The subset of ``kafka.KafkaProducer``'s interface this module needs."""

    def send(self, topic: str, key: bytes, value: bytes) -> Any: ...

    def flush(self) -> None: ...


def serialize_frame(frame: Frame) -> bytes:
    """Serialize a `Frame` to a JSON byte-string carrying its metadata.

    Includes ``stream_id``, ``timestamp`` (ISO-8601, UTC) and ``sequence``
    (per the Kafka issue's acceptance criteria) plus the frame's resolution
    and base64-encoded pixel payload so a downstream consumer can fully
    reconstruct the frame.
    """
    payload = {
        "stream_id": frame.stream_id,
        "sequence": frame.sequence,
        "timestamp": frame.captured_at.isoformat(),
        "width": frame.width,
        "height": frame.height,
        "pixels_base64": base64.b64encode(frame.pixels).decode("ascii"),
    }
    return json.dumps(payload).encode("utf-8")


def partition_key(frame: Frame) -> bytes:
    """Key used to route a frame's message: its ``stream_id``.

    Keying by stream id (rather than e.g. a random or round-robin key) is
    what preserves per-stream ordering — see the module docstring.
    """
    return frame.stream_id.encode("utf-8")


@dataclass
class PublishResult:
    stream_id: str
    sequence: int
    topic: str


class FramePublisher:
    """Publishes `Frame` objects to a Kafka topic, keyed for per-stream ordering."""

    def __init__(self, producer: KafkaProducerLike, topic: str = DEFAULT_TOPIC) -> None:
        self._producer = producer
        self._topic = topic

    @classmethod
    def for_bootstrap_servers(cls, bootstrap_servers: str, topic: str = DEFAULT_TOPIC) -> "FramePublisher":
        """Build a publisher backed by a real Kafka broker.

        Requires the optional ``kafka-python`` dependency and a reachable
        broker (e.g. the one started by ``docker/docker-compose.yml``); it is
        not imported at module load time so the rest of this package stays
        importable without Kafka installed.
        """
        from kafka import KafkaProducer  # local import: optional dependency

        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        return cls(producer, topic=topic)

    def publish(self, frame: Frame) -> PublishResult:
        self._producer.send(self._topic, key=partition_key(frame), value=serialize_frame(frame))
        return PublishResult(stream_id=frame.stream_id, sequence=frame.sequence, topic=self._topic)

    def flush(self) -> None:
        self._producer.flush()
