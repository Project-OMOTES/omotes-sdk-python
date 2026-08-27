# ruff: noqa: D103

from datetime import UTC

from omotes_sdk.log_forwarding import SolverStdoutCapture


def test_solver_stdout_capture_buffers_until_newline() -> None:
    entries = []
    capture = SolverStdoutCapture(entries.append)

    written_a = capture.write(b"partial")
    written_b = capture.write(b" line\n")

    assert written_a == len(b"partial")
    assert written_b == len(b" line\n")
    assert len(entries) == 1
    timestamp, line = entries[0]
    assert timestamp.tzinfo == UTC
    assert line == "partial line"


def test_solver_stdout_capture_ignores_python_formatted_log_lines() -> None:
    entries = []
    capture = SolverStdoutCapture(entries.append)

    capture.write(b"12:00:00.123 | INFO    | hidden\n")
    capture.write(b"2026-07-24 12:00:00,123 [hidden]\n")
    capture.write(b"visible line\n")

    assert [line for _, line in entries] == ["visible line"]


def test_solver_stdout_capture_flushes_remaining_buffer() -> None:
    entries = []
    capture = SolverStdoutCapture(entries.append)

    capture.write(b"without newline")
    assert entries == []

    capture.flush_remaining()

    assert [line for _, line in entries] == ["without newline"]
