import pytest

from video_platform import MultiStreamSimulator


def test_generated_streams_have_stable_unique_ids_and_frame_shape():
    simulator = MultiStreamSimulator.generated(2, fps=12, width=8, height=6)

    assert simulator.stream_ids == ("camera-0001", "camera-0002")
    simulator.start("camera-0001")
    frame = next(simulator.frames("camera-0001", 1))
    assert (frame.width, frame.height) == (8, 6)
    assert len(frame.pixels) == 48


def test_one_stream_can_stop_without_affecting_another():
    simulator = MultiStreamSimulator.generated(2, fps=10, width=4, height=3)
    simulator.start("camera-0001")
    simulator.start("camera-0002")
    simulator.stop("camera-0001")

    assert list(simulator.frames("camera-0001", 1)) == []
    assert len(list(simulator.frames("camera-0002", 2))) == 2


def test_restart_preserves_monotonic_sequence():
    simulator = MultiStreamSimulator.generated(1, fps=10, width=2, height=2)
    simulator.start("camera-0001")
    first = next(simulator.frames("camera-0001", 1))
    simulator.stop("camera-0001")
    simulator.start("camera-0001")
    second = next(simulator.frames("camera-0001", 1))

    assert (first.sequence, second.sequence) == (0, 1)


@pytest.mark.parametrize("kwargs", [{"count": 0}, {"count": 1, "fps": 0}])
def test_invalid_explicit_configuration_fails(kwargs):
    defaults = {"count": 1, "fps": 10, "width": 4, "height": 3}
    defaults.update(kwargs)
    with pytest.raises(ValueError):
        MultiStreamSimulator.generated(**defaults)
