import asyncio
import io
import logging
import queue
import re
import sys
import threading
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, TextIO

import streamcapture

from omotes_sdk.prefect_util import get_client, get_run_context

LogEntry = tuple[datetime, str]

_FORWARDER_QUEUE_TIMEOUT_SECONDS = 0.05
_FORWARDER_BATCH_SIZE = 10
_PYTHON_LOG_LINE_RE = re.compile(
    r"^(?:\d{2}:\d{2}:\d{2}\.\d{3} \| [A-Z]+\s+\||\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} \[)"
)


class SolverStdoutCapture(io.RawIOBase):
    """Capture solver stdout lines and pass normalized entries to a callback."""

    def __init__(self, log_fn: Callable[[LogEntry], None]) -> None:
        """Initialize the line-capture sink.

        Args:
            log_fn: Callback that receives normalized log entries.

        """
        self._log_fn = log_fn
        self._buffer = b""

    def write(self, data, /) -> int:  # noqa: ANN001
        """Buffer incoming bytes and emit complete lines.

        Args:
            data: Byte chunk written to the stream.

        Returns:
            int: Number of bytes consumed.

        """
        chunk = bytes(data)
        self._buffer += chunk
        while b"\n" in self._buffer:
            raw_line, self._buffer = self._buffer.split(b"\n", 1)
            self._emit(raw_line)
        return len(chunk)

    def flush_remaining(self) -> None:
        """Emit any trailing buffered data as a final log line."""
        if self._buffer:
            self._emit(self._buffer)
            self._buffer = b""

    def _emit(self, raw_line: bytes) -> None:
        line = raw_line.decode(errors="replace").rstrip()
        if not line:
            return
        if _PYTHON_LOG_LINE_RE.match(line):
            return
        self._log_fn((datetime.now(UTC), line))


class BatchedLogForwarder:
    """Forward captured log entries asynchronously in small batches."""

    def __init__(
        self,
        send_batch: Callable[[list[LogEntry]], Coroutine[Any, Any, None]],
        queue_timeout_seconds: float = _FORWARDER_QUEUE_TIMEOUT_SECONDS,
        batch_size: int = _FORWARDER_BATCH_SIZE,
    ) -> None:
        """Initialize a background log forwarder.

        Args:
            send_batch: Async callback that sends a batch of entries.
            queue_timeout_seconds: Maximum wait before flushing pending entries.
            batch_size: Maximum entries sent per batch.

        """
        self._send_batch = send_batch
        self._queue_timeout_seconds = queue_timeout_seconds
        self._batch_size = batch_size
        self._queue: queue.Queue[LogEntry | None] = queue.Queue()
        self._thread = threading.Thread(target=self._run, name="solver-log-forwarder")

    def start(self) -> None:
        """Start the background forwarding thread."""
        self._thread.start()

    def enqueue(self, entry: LogEntry) -> None:
        """Queue a single log entry for forwarding."""
        self._queue.put_nowait(entry)

    def stop(self) -> None:
        """Stop the forwarding thread after flushing pending entries."""
        self._queue.put(None)
        self._thread.join()

    def _run(self) -> None:
        pending: list[LogEntry] = []

        def _flush_pending() -> None:
            nonlocal pending
            if not pending:
                return
            asyncio.run(self._send_batch(pending))
            pending = []

        while True:
            try:
                entry = self._queue.get(timeout=self._queue_timeout_seconds)
            except queue.Empty:
                self._safe_flush(_flush_pending)
                continue

            if entry is None:
                break

            pending.append(entry)
            if len(pending) >= self._batch_size:
                self._safe_flush(_flush_pending)

        self._safe_flush(_flush_pending)

    @staticmethod
    def _safe_flush(flush_fn: Callable[[], None]) -> None:
        try:
            flush_fn()
        except Exception as exc:
            print(f"Solver log forwarding failed: {exc}", file=sys.__stderr__, flush=True)


def make_prefect_log_sender(
    name: str = "solver",
    level: int = logging.INFO,
) -> Callable[[list[LogEntry]], Coroutine[Any, Any, None]]:
    """Build an async sender that writes batched log entries to Prefect.

    Returns:
        Callable[[list[LogEntry]], Coroutine[Any, Any, None]]: Async batch sender.

    Raises:
        RuntimeError: If no flow run id is available in the current Prefect context.

    """
    run_context = get_run_context()
    flow_run_obj = getattr(run_context, "flow_run", None)
    flow_run_id = str(flow_run_obj.id) if flow_run_obj is not None else None
    if flow_run_id is None:
        raise RuntimeError("Missing flow run id in Prefect run context")

    async def _send(entries: list[LogEntry]) -> None:
        async with get_client() as client:
            await client.create_logs([
                {
                    "name": name,
                    "level": level,
                    "message": line,
                    "timestamp": ts.isoformat(),
                    "flow_run_id": flow_run_id,
                    "task_run_id": None,
                }
                for ts, line in entries
            ])

    return _send


class StdCaptureToLogSession:
    """Context manager that captures solver stdout and forwards it in batches."""

    def __init__(
        self,
        capture_stderr: bool = True,
        name: str = "solver",
        level: int = logging.INFO,
    ) -> None:
        """Initialize a capture session.

        Args:
            capture_stderr: Whether stderr should be captured alongside stdout.
            name: Prefect log name.
            level: Prefect log level.

        """
        send_batch = make_prefect_log_sender(name=name, level=level)
        self._capture_stderr = capture_stderr
        self._forwarder = BatchedLogForwarder(send_batch)
        self._line_captures: list[SolverStdoutCapture] = []
        self._stream_targets: list[tuple[TextIO, SolverStdoutCapture]] = []
        self._active_stream_captures: list[streamcapture.StreamCapture] = []

        stdout_capture = SolverStdoutCapture(self._forwarder.enqueue)
        self._line_captures.append(stdout_capture)
        self._stream_targets.append((sys.stdout, stdout_capture))

        if self._capture_stderr:
            stderr_capture = SolverStdoutCapture(self._forwarder.enqueue)
            self._line_captures.append(stderr_capture)
            self._stream_targets.append((sys.stderr, stderr_capture))

    def __enter__(self) -> "StdCaptureToLogSession":
        """Start stream capture and return the active context manager.

        Returns:
            StdCaptureToLogSession: Active capture session.

        """
        self._forwarder.start()
        self._active_stream_captures = [
            streamcapture.StreamCapture(source, target, echo=True) for source, target in self._stream_targets
        ]
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Stop stream capture and flush pending log lines.

        Args:
            exc_type: Exception type raised in the managed block, if any.
            exc_val: Exception instance raised in the managed block, if any.
            exc_tb: Exception traceback raised in the managed block, if any.

        """
        for stream_capture in self._active_stream_captures:
            stream_capture.close()
        for line_capture in self._line_captures:
            line_capture.flush_remaining()
        self._forwarder.stop()
