from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Self, cast

_SESSION_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")

_REGISTRY_LOCK_TIMEOUT_SECONDS = 5.0
_REGISTRY_LOCK_RETRY_INTERVAL_SECONDS = 0.05


class SessionBusyError(RuntimeError):
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        super().__init__(f"Session is already open: {session_id}")


class SessionRegistryBusyError(RuntimeError):
    def __init__(
        self,
        registry: Path,
        *,
        holder_pid: int | None = None,
        holder_since: str | None = None,
    ) -> None:
        self.registry = registry
        self.holder_pid = holder_pid
        self.holder_since = holder_since
        holder = ""
        if holder_pid is not None:
            holder = f" (held by pid {holder_pid}"
            if holder_since is not None:
                holder += f" since {holder_since}"
            holder += ")"
        super().__init__(
            f"Session store is busy{holder}: {registry}; "
            "another session operation may be stuck — retry shortly"
        )


class SessionLease:
    def __init__(self, root: Path, session_id: str) -> None:
        if _SESSION_ID_PATTERN.fullmatch(session_id) is None:
            raise ValueError(f"invalid session ID: {session_id!r}")
        self._path = root / "active" / f"{session_id}.lock"
        self._session_id = session_id
        self._file: Any | None = None

    @property
    def path(self) -> Path:
        return self._path

    def acquire(self) -> Self:
        if self._file is not None:
            raise RuntimeError("session lease is already acquired")
        if self._path.parents[1].is_symlink() or self._path.parent.is_symlink():
            raise ValueError("session lease path cannot contain a symbolic link")
        self._path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with _lease_directory_lock(self._path.parent):
            # The lock file names the session and the pid holding it, so it is
            # created owner-only; an existing file keeps its current mode.
            self._path.touch(mode=0o600, exist_ok=True)
            file = self._path.open("a+b")
            try:
                _acquire_file_lock(file)
            except BlockingIOError as exc:
                file.close()
                raise SessionBusyError(self._session_id) from exc
            diagnostic = {
                "lease_version": 1,
                "session_id": self._session_id,
                "process_id": os.getpid(),
                "acquired_at": _timestamp(),
            }
            file.seek(0)
            file.truncate()
            file.write(
                json.dumps(
                    diagnostic,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                + b"\n"
            )
            file.flush()
            os.fsync(file.fileno())
            self._file = file
        return self

    def release(self) -> None:
        if self._file is None:
            return
        file = self._file
        self._file = None
        with _lease_directory_lock(self._path.parent):
            try:
                _release_file_lock(file)
            finally:
                file.close()
            self._path.unlink(missing_ok=True)

    def __enter__(self) -> Self:
        return self.acquire()

    def __exit__(self, *_exc: object) -> None:
        self.release()


@contextmanager
def _lease_directory_lock(directory: Path) -> Iterator[None]:
    registry = directory / ".registry"
    registry.touch(mode=0o600, exist_ok=True)
    file = registry.open("a+b")
    try:
        _acquire_registry_lock(registry, file)
    except BaseException:
        file.close()
        raise
    try:
        _record_registry_holder(file)
        yield
    finally:
        _clear_registry_holder(file)
        _release_file_lock(file)
        file.close()


def _acquire_registry_lock(registry: Path, file: Any) -> None:
    deadline = time.monotonic() + _REGISTRY_LOCK_TIMEOUT_SECONDS
    while True:
        try:
            _acquire_file_lock(file)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                holder_pid, holder_since = _read_registry_holder(registry)
                raise SessionRegistryBusyError(
                    registry, holder_pid=holder_pid, holder_since=holder_since
                ) from None
            time.sleep(_REGISTRY_LOCK_RETRY_INTERVAL_SECONDS)


def _record_registry_holder(file: Any) -> None:
    diagnostic = {
        "registry_version": 1,
        "process_id": os.getpid(),
        "acquired_at": _timestamp(),
    }
    file.seek(0)
    file.truncate()
    file.write(
        json.dumps(
            diagnostic, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
    file.flush()
    os.fsync(file.fileno())


def _clear_registry_holder(file: Any) -> None:
    file.seek(0)
    file.truncate()
    file.flush()
    os.fsync(file.fileno())


def _read_registry_holder(registry: Path) -> tuple[int | None, str | None]:
    try:
        content = registry.read_text(encoding="utf-8")
    except OSError:
        return None, None
    try:
        diagnostic = json.loads(content)
    except ValueError:
        return None, None
    if not isinstance(diagnostic, dict):
        return None, None
    holder_pid = diagnostic.get("process_id")
    holder_since = diagnostic.get("acquired_at")
    return (
        holder_pid if isinstance(holder_pid, int) else None,
        holder_since if isinstance(holder_since, str) else None,
    )


def _acquire_file_lock(file: Any, *, blocking: bool = False) -> None:
    if _is_windows():
        msvcrt = cast(Any, __import__("msvcrt"))

        file.seek(0)
        if file.read(1) == b"":
            file.write(b"\0")
            file.flush()
        file.seek(0)
        try:
            mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
            msvcrt.locking(file.fileno(), mode, 1)
        except OSError as exc:
            raise BlockingIOError from exc
        return
    import fcntl

    try:
        operation = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
        fcntl.flock(file.fileno(), operation)
    except OSError as exc:
        raise BlockingIOError from exc


def _release_file_lock(file: Any) -> None:
    if _is_windows():
        msvcrt = cast(Any, __import__("msvcrt"))

        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(file.fileno(), fcntl.LOCK_UN)


def _is_windows() -> bool:
    return os.name == "nt"


def _timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


__all__ = ["SessionBusyError", "SessionLease", "SessionRegistryBusyError"]
