from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator


@dataclass(frozen=True)
class StreamConfig:
    stream_id: str
    fps: float = 10.0
    width: int = 320
    height: int = 180

    def __post_init__(self) -> None:
        if not self.stream_id.strip():
            raise ValueError("stream_id must not be empty")
        if self.fps <= 0 or self.fps > 240:
            raise ValueError("fps must be greater than 0 and at most 240")
        if self.width <= 0 or self.height <= 0:
            raise ValueError("width and height must be positive")


@dataclass(frozen=True)
class Frame:
    stream_id: str
    sequence: int
    captured_at: datetime
    width: int
    height: int
    pixels: bytes


class MultiStreamSimulator:
    """Independent synthetic camera feeds with stable identifiers."""

    def __init__(self, configs: list[StreamConfig]) -> None:
        if not configs:
            raise ValueError("at least one stream is required")
        ids = [config.stream_id for config in configs]
        if len(ids) != len(set(ids)):
            raise ValueError("stream_id values must be unique")
        self._configs = {config.stream_id: config for config in configs}
        self._running = {stream_id: False for stream_id in ids}
        self._sequences = {stream_id: 0 for stream_id in ids}

    @classmethod
    def generated(cls, count: int, *, fps: float, width: int, height: int):
        if count <= 0:
            raise ValueError("stream count must be positive")
        return cls([
            StreamConfig(f"camera-{index:04d}", fps=fps, width=width, height=height)
            for index in range(1, count + 1)
        ])

    @property
    def stream_ids(self) -> tuple[str, ...]:
        return tuple(self._configs)

    def start(self, stream_id: str) -> None:
        self._require_stream(stream_id)
        self._running[stream_id] = True

    def stop(self, stream_id: str) -> None:
        self._require_stream(stream_id)
        self._running[stream_id] = False

    def is_running(self, stream_id: str) -> bool:
        self._require_stream(stream_id)
        return self._running[stream_id]

    def frames(self, stream_id: str, count: int) -> Iterator[Frame]:
        config = self._require_stream(stream_id)
        if count < 0:
            raise ValueError("frame count must not be negative")
        for _ in range(count):
            if not self._running[stream_id]:
                return
            sequence = self._sequences[stream_id]
            self._sequences[stream_id] += 1
            value = sequence % 256
            yield Frame(
                stream_id=stream_id,
                sequence=sequence,
                captured_at=datetime.now(timezone.utc),
                width=config.width,
                height=config.height,
                pixels=bytes([value]) * (config.width * config.height),
            )

    def _require_stream(self, stream_id: str) -> StreamConfig:
        try:
            return self._configs[stream_id]
        except KeyError as exc:
            raise KeyError(f"unknown stream_id: {stream_id}") from exc
