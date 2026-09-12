"""Tests for the YAML-configurable ingestion service (issue #3)."""

from __future__ import annotations

import pytest

from video_platform import IngestionConfigError, load_ingestion_config


def _write(tmp_path, text, name="ingestion.yaml"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


VALID = """
streams: 5
fps: 15
width: 640
height: 480
kafka:
  bootstrap_servers: localhost:9092
  topic: custom-topic
"""


def test_valid_config_loads_all_fields(tmp_path):
    config = load_ingestion_config(_write(tmp_path, VALID))
    assert config.streams == 5
    assert config.fps == 15.0
    assert (config.width, config.height) == (640, 480)
    assert config.kafka.bootstrap_servers == "localhost:9092"
    assert config.kafka.topic == "custom-topic"


def test_defaults_apply_when_fields_omitted(tmp_path):
    config = load_ingestion_config(_write(tmp_path, "streams: 3\n"))
    assert config.streams == 3
    assert config.fps == 10.0
    assert (config.width, config.height) == (320, 180)
    assert config.kafka.bootstrap_servers is None
    assert config.kafka.topic == "video-frames"


def test_missing_file_raises(tmp_path):
    with pytest.raises(IngestionConfigError, match="not found"):
        load_ingestion_config(tmp_path / "nope.yaml")


def test_bad_yaml_reports_clearly(tmp_path):
    with pytest.raises(IngestionConfigError, match="not valid YAML"):
        load_ingestion_config(_write(tmp_path, "streams: [1, 2\n"))


def test_top_level_list_rejected(tmp_path):
    with pytest.raises(IngestionConfigError, match="mapping"):
        load_ingestion_config(_write(tmp_path, "- a\n- b\n"))


def test_unknown_top_level_field_rejected(tmp_path):
    with pytest.raises(IngestionConfigError, match="unknown"):
        load_ingestion_config(_write(tmp_path, "streams: 1\nbogus: true\n"))


def test_unknown_kafka_field_rejected(tmp_path):
    with pytest.raises(IngestionConfigError, match="unknown kafka"):
        load_ingestion_config(_write(tmp_path, "streams: 1\nkafka:\n  bogus: 1\n"))


@pytest.mark.parametrize("field,value", [("streams", 0), ("streams", -1), ("fps", 0), ("fps", 500)])
def test_out_of_range_numeric_fields_rejected(tmp_path, field, value):
    with pytest.raises(IngestionConfigError):
        load_ingestion_config(_write(tmp_path, f"{field}: {value}\n"))


def test_non_numeric_streams_rejected(tmp_path):
    with pytest.raises(IngestionConfigError, match="streams"):
        load_ingestion_config(_write(tmp_path, "streams: not-a-number\n"))


def test_boolean_is_not_accepted_as_numeric_field(tmp_path):
    # bool is a subclass of int in Python -- must not silently pass as streams/fps.
    with pytest.raises(IngestionConfigError, match="streams"):
        load_ingestion_config(_write(tmp_path, "streams: true\n"))


def test_zero_or_negative_width_height_rejected(tmp_path):
    with pytest.raises(IngestionConfigError, match="width/height"):
        load_ingestion_config(_write(tmp_path, "streams: 1\nwidth: 0\n"))


def test_empty_kafka_topic_rejected(tmp_path):
    with pytest.raises(IngestionConfigError, match="topic"):
        load_ingestion_config(_write(tmp_path, "streams: 1\nkafka:\n  topic: ''\n"))
