import json

from video_platform import DEFAULT_TOPIC, FramePublisher, MultiStreamSimulator, partition_key, serialize_frame


class FakeKafkaProducer:
    """In-memory stand-in for kafka.KafkaProducer used by the tests.

    No real broker or the kafka-python client library is involved: this
    fake just records every call so the tests can assert on serialization
    and key selection without hardware or paid services.
    """

    def __init__(self) -> None:
        self.sent = []
        self.flushed = False

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self) -> None:
        self.flushed = True


def _frame():
    simulator = MultiStreamSimulator.generated(1, fps=10, width=2, height=2)
    stream_id = simulator.stream_ids[0]
    simulator.start(stream_id)
    return next(simulator.frames(stream_id, 1))


def test_serialize_frame_includes_required_fields():
    frame = _frame()

    payload = json.loads(serialize_frame(frame))

    assert payload["stream_id"] == frame.stream_id
    assert payload["sequence"] == frame.sequence
    assert payload["timestamp"] == frame.captured_at.isoformat()
    assert payload["width"] == frame.width
    assert payload["height"] == frame.height
    assert "pixels_base64" in payload


def test_partition_key_is_stream_id_bytes():
    frame = _frame()

    assert partition_key(frame) == frame.stream_id.encode("utf-8")


def test_publish_sends_keyed_message_to_configured_topic():
    producer = FakeKafkaProducer()
    publisher = FramePublisher(producer, topic="custom-topic")
    frame = _frame()

    result = publisher.publish(frame)

    assert len(producer.sent) == 1
    topic, key, value = producer.sent[0]
    assert topic == "custom-topic"
    assert key == frame.stream_id.encode("utf-8")
    assert json.loads(value)["sequence"] == frame.sequence
    assert result.stream_id == frame.stream_id
    assert result.topic == "custom-topic"


def test_publish_defaults_to_default_topic():
    producer = FakeKafkaProducer()
    publisher = FramePublisher(producer)
    frame = _frame()

    publisher.publish(frame)

    assert producer.sent[0][0] == DEFAULT_TOPIC


def test_frames_from_same_stream_share_a_key_preserving_per_stream_ordering():
    simulator = MultiStreamSimulator.generated(2, fps=10, width=2, height=2)
    for stream_id in simulator.stream_ids:
        simulator.start(stream_id)
    producer = FakeKafkaProducer()
    publisher = FramePublisher(producer)

    for stream_id in simulator.stream_ids:
        for frame in simulator.frames(stream_id, 3):
            publisher.publish(frame)

    keys_by_stream = {}
    for _, key, value in producer.sent:
        payload = json.loads(value)
        keys_by_stream.setdefault(payload["stream_id"], set()).add(key)

    # Every message for a given stream carries the same key, so Kafka's
    # default partitioner always routes it to the same partition.
    assert all(len(keys) == 1 for keys in keys_by_stream.values())
    # Different streams get different keys, so they need not land on the
    # same partition (and therefore need not be ordered relative to each
    # other).
    assert len({next(iter(keys)) for keys in keys_by_stream.values()}) == 2


def test_flush_delegates_to_producer():
    producer = FakeKafkaProducer()
    publisher = FramePublisher(producer)

    publisher.flush()

    assert producer.flushed is True
