"""YAML-driven ingestion configuration (Phase 1).

`configs/ingestion.yaml` (or any path passed via `--config`) controls stream
count, fps, resolution, and Kafka connection settings for the ingestion CLI
without a code change: edit the file, restart the service, ingestion
behavior changes. A malformed or out-of-range value fails fast at load time
with a message naming the bad field, rather than surfacing later as a
confusing simulator/publisher error.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_KAFKA_TOPIC = "video-frames"


class IngestionConfigError(ValueError):
    """The ingestion config file is missing, unparseable, or has invalid values."""


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str | None = None
    topic: str = DEFAULT_KAFKA_TOPIC

    def __post_init__(self) -> None:
        if not self.topic.strip():
            raise IngestionConfigError("kafka.topic must not be empty")


@dataclass(frozen=True)
class IngestionConfig:
    streams: int
    fps: float
    width: int
    height: int
    kafka: KafkaSettings

    def __post_init__(self) -> None:
        if self.streams <= 0:
            raise IngestionConfigError(f"streams must be positive, got {self.streams!r}")
        if self.fps <= 0 or self.fps > 240:
            raise IngestionConfigError(f"fps must be > 0 and <= 240, got {self.fps!r}")
        if self.width <= 0 or self.height <= 0:
            raise IngestionConfigError(
                f"width/height must be positive, got {self.width!r}x{self.height!r}"
            )


def _require_type(value: Any, types: tuple[type, ...], field: str) -> Any:
    if isinstance(value, bool) or not isinstance(value, types):
        raise IngestionConfigError(
            f"{field} must be one of {[t.__name__ for t in types]}, got {value!r}"
        )
    return value


def load_ingestion_config(path: str | Path) -> IngestionConfig:
    """Parse and validate an ingestion YAML config file.

    Raises :class:`IngestionConfigError` for a missing file, invalid YAML, a
    non-mapping top level, an unknown/missing field, or any value that fails
    :class:`IngestionConfig`'s own range checks.
    """
    path = Path(path)
    if not path.is_file():
        raise IngestionConfigError(f"ingestion config file not found: {path}")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise IngestionConfigError(f"{path.name} is not valid YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise IngestionConfigError(f"{path.name} must contain a YAML mapping at the top level")

    unknown = set(raw) - {"streams", "fps", "width", "height", "kafka"}
    if unknown:
        raise IngestionConfigError(f"unknown config field(s) in {path.name}: {sorted(unknown)}")

    try:
        streams = _require_type(raw.get("streams", 1), (int,), "streams")
        fps = _require_type(raw.get("fps", 10.0), (int, float), "fps")
        width = _require_type(raw.get("width", 320), (int,), "width")
        height = _require_type(raw.get("height", 180), (int,), "height")

        kafka_raw = raw.get("kafka", {}) or {}
        if not isinstance(kafka_raw, dict):
            raise IngestionConfigError("kafka must be a mapping")
        kafka_unknown = set(kafka_raw) - {"bootstrap_servers", "topic"}
        if kafka_unknown:
            raise IngestionConfigError(f"unknown kafka field(s): {sorted(kafka_unknown)}")

        kafka = KafkaSettings(
            bootstrap_servers=kafka_raw.get("bootstrap_servers"),
            topic=kafka_raw.get("topic", DEFAULT_KAFKA_TOPIC),
        )
        return IngestionConfig(streams=streams, fps=float(fps), width=width, height=height, kafka=kafka)
    except IngestionConfigError:
        raise
    except (TypeError, ValueError) as exc:
        raise IngestionConfigError(f"invalid value in {path.name}: {exc}") from exc
