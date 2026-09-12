"""Tests for the ingestion CLI's --config support (issue #3)."""

from __future__ import annotations

import json
import sys

import pytest

from video_platform.cli import main


def _write_config(tmp_path, text, name="ingestion.yaml"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def test_config_file_drives_stream_count_and_resolution(tmp_path, monkeypatch, capsys):
    config_path = _write_config(tmp_path, "streams: 2\nfps: 20\nwidth: 64\nheight: 48\n")
    monkeypatch.setattr(sys, "argv", ["video-ingest", "--config", str(config_path), "--frames", "1"])

    rc = main()
    assert rc == 0

    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert {entry["stream_id"] for entry in lines} == {"camera-0001", "camera-0002"}
    assert all(entry["resolution"] == [64, 48] for entry in lines)


def test_config_file_overrides_cli_flag_defaults(tmp_path, monkeypatch, capsys):
    # --streams default is 1; the config file's value must win.
    config_path = _write_config(tmp_path, "streams: 3\n")
    monkeypatch.setattr(
        sys, "argv", ["video-ingest", "--config", str(config_path), "--streams", "1", "--frames", "1"]
    )

    main()
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 3


def test_invalid_config_fails_fast_with_clear_error(tmp_path, monkeypatch, capsys):
    config_path = _write_config(tmp_path, "streams: 0\n")
    monkeypatch.setattr(sys, "argv", ["video-ingest", "--config", str(config_path)])

    with pytest.raises(SystemExit):
        main()
    assert "streams" in capsys.readouterr().err


def test_no_config_falls_back_to_individual_flags(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["video-ingest", "--streams", "1", "--frames", "1"])
    rc = main()
    assert rc == 0
    line = json.loads(capsys.readouterr().out.strip())
    assert line["stream_id"] == "camera-0001"
