import argparse
import json

from .simulator import MultiStreamSimulator


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(description="Run deterministic synthetic camera feeds.")
    parser.add_argument("--streams", type=positive_int, default=1)
    parser.add_argument("--frames", type=positive_int, default=3)
    parser.add_argument("--fps", type=float, default=10.0)
    parser.add_argument("--width", type=positive_int, default=320)
    parser.add_argument("--height", type=positive_int, default=180)
    args = parser.parse_args()

    try:
        simulator = MultiStreamSimulator.generated(
            args.streams, fps=args.fps, width=args.width, height=args.height
        )
    except ValueError as exc:
        parser.error(str(exc))

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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
