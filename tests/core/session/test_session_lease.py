from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

import vibe.core.session.session_lease as lease_module
from vibe.core.session.session_lease import (
    SessionBusyError,
    SessionLease,
    SessionRegistryBusyError,
)

SESSION_ID = "019ffb1e-741d-7f90-84df-ef66011876ca"


def test_session_lease_is_exclusive_and_recoverable(tmp_path: Path) -> None:
    first = SessionLease(tmp_path, SESSION_ID).acquire()
    try:
        with pytest.raises(SessionBusyError):
            SessionLease(tmp_path, SESSION_ID).acquire()
    finally:
        first.release()

    assert not first.path.exists()
    SessionLease(tmp_path, SESSION_ID).acquire().release()


def test_session_lease_rejects_a_path_shaped_identity(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="invalid session ID"):
        SessionLease(tmp_path, "../escape")


def test_session_lease_accepts_a_safe_legacy_identity(tmp_path: Path) -> None:
    lease = SessionLease(tmp_path, "resumable-with-stats").acquire()

    assert lease.path == tmp_path / "active" / "resumable-with-stats.lock"
    lease.release()


@pytest.mark.parametrize(("blocking", "expected_mode"), [(False, 2), (True, 1)])
def test_windows_locking_uses_a_one_byte_region(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, blocking: bool, expected_mode: int
) -> None:
    calls: list[tuple[int, int]] = []
    fake_msvcrt = SimpleNamespace(
        LK_LOCK=1,
        LK_NBLCK=2,
        LK_UNLCK=0,
        locking=lambda _descriptor, mode, length: calls.append((mode, length)),
    )
    monkeypatch.setattr(lease_module, "_is_windows", lambda: True)
    monkeypatch.setitem(sys.modules, "msvcrt", fake_msvcrt)
    path = tmp_path / "lease.lock"

    with path.open("w+b") as file:
        lease_module._acquire_file_lock(file, blocking=blocking)
        lease_module._release_file_lock(file)

    assert calls == [(expected_mode, 1), (fake_msvcrt.LK_UNLCK, 1)]


def test_session_lease_rejects_a_symlinked_active_namespace(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "active").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symbolic link"):
        SessionLease(tmp_path, SESSION_ID).acquire()


_REGISTRY_HOLDER_HELPER = """
import sys
import time
from pathlib import Path
from vibe.core.session import session_lease

registry = Path(sys.argv[1]) / "active" / ".registry"
registry.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
registry.touch(mode=0o600)
holder = registry.open("a+b")
session_lease._acquire_file_lock(holder)
session_lease._record_registry_holder(holder)
time.sleep(30)
"""


def test_acquire_times_out_when_the_registry_is_held(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(lease_module, "_REGISTRY_LOCK_TIMEOUT_SECONDS", 0.2)
    active = tmp_path / "active"
    active.mkdir()
    holder = (active / ".registry").open("a+b")
    try:
        lease_module._acquire_file_lock(holder)
        started = time.monotonic()
        with pytest.raises(SessionRegistryBusyError):
            SessionLease(tmp_path, SESSION_ID).acquire()
        assert time.monotonic() - started < 4.0
    finally:
        holder.close()


def test_acquire_names_the_registry_holder(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(lease_module, "_REGISTRY_LOCK_TIMEOUT_SECONDS", 0.2)
    helper = subprocess.Popen(
        [sys.executable, "-c", _REGISTRY_HOLDER_HELPER, str(tmp_path), SESSION_ID],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        registry = tmp_path / "active" / ".registry"
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if helper.poll() is not None:
                pytest.fail(f"holder helper exited early: {helper.returncode}")
            if registry.exists() and registry.read_bytes() != b"":
                break
            time.sleep(0.01)
        else:
            pytest.fail("holder helper never recorded its diagnostic")
        started = time.monotonic()
        with pytest.raises(SessionRegistryBusyError) as excinfo:
            SessionLease(tmp_path, SESSION_ID).acquire()
        assert time.monotonic() - started < 4.0
        error = excinfo.value
        assert error.holder_pid == helper.pid
        assert error.holder_since is not None
        assert str(helper.pid) in str(error)
    finally:
        helper.kill()
        helper.wait()


def test_release_is_bounded_when_the_registry_is_held(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(lease_module, "_REGISTRY_LOCK_TIMEOUT_SECONDS", 0.2)
    lease = SessionLease(tmp_path, SESSION_ID).acquire()
    holder = (tmp_path / "active" / ".registry").open("a+b")
    try:
        lease_module._acquire_file_lock(holder)
        started = time.monotonic()
        with pytest.raises(SessionRegistryBusyError):
            lease.release()
        assert time.monotonic() - started < 4.0
    finally:
        holder.close()
        lease.path.unlink(missing_ok=True)
