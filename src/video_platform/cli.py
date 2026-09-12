import argparse
import json

from .config import DEFAULT_KAFKA_TOPIC, IngestionConfig, IngestionConfigError, KafkaSettings, load_ingestion_config
from .kafka_publisher import FramePublisher
from .simulator import MultiStreamSimulator


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _resolve_config(args: argparse.Namespace) -> IngestionConfig:
    """Build the effective ingestion config: --config file, else individual flags.

    A config file takes priority over the flag defaults so that "edit the
    YAML and restart" is the documented way to reproduce ingestion
    experiments at different scales with no code change.
    """
    if args.config:
        return load_ingestion_config(args.config)
    return IngestionConfig(
        streams=args.streams,
        fps=args.fps,
        width=args.width,
        height=args.height,
        kafka=KafkaSettings(bootstrap_servers=args.kafka_bootstrap_servers, topic=args.kafka_topic),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run deterministic synthetic camera feeds.")
    parser.add_argument(
        "--config",
        default=None,
        help="path to a YAML ingestion config (see configs/ingestion.yaml); "
        "overrides --streams/--fps/--width/--height/--kafka-* when given",
    )
    parser.add_argument("--streams", type=positive_int, default=1)
    parser.add_argument("--frames", type=positive_int, default=3)
    parser.add_argument("--fps", type=float, default=10.0)
    parser.add_argument("--width", type=positive_int, default=320)
    parser.add_argument("--height", type=positive_int, default=180)
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=None,
        help="host:port of a Kafka broker (e.g. localhost:9092). "
        "When set, frames are also published to --kafka-topic. "
        "See docker/docker-compose.yml for a local dev broker.",
    )
    parser.add_argument("--kafka-topic", default=DEFAULT_KAFKA_TOPIC)
    args = parser.parse_args()

    try:
        config = _resolve_config(args)
    except IngestionConfigError as exc:
        parser.error(str(exc))

    try:
        simulator = MultiStreamSimulator.generated(
            config.streams, fps=config.fps, width=config.width, height=config.height
        )
    except ValueError as exc:
        parser.error(str(exc))

    publisher = None
    if config.kafka.bootstrap_servers:
        publisher = FramePublisher.for_bootstrap_servers(
            config.kafka.bootstrap_servers, topic=config.kafka.topic
        )

    for stream_id in simulator.stream_ids:
        simulator.start(stream_id)
    for stream_id in simulator.stream_ids:
        for frame in simulator.frames(stream_id, args.frames):
            print(json.dumps({
                "stream_id": frame.stream_id,
                "sequence": frame.sequence,
                "captured_at": frame.captured_at.isoformat(),
                "resolution": [frame.width, frame.height],
                "bytes": len(frame.pixels),
            }))
            if publisher is not None:
                publisher.publish(frame)
    if publisher is not None:
        publisher.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
