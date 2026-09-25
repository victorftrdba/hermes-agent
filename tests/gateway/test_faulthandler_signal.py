"""Regression: SIGUSR2 faulthandler registration must not chain to the default action.

``chain=True`` handed SIGUSR2 to the previous handler after the stack dump; on macOS that
terminated the gateway, turning a diagnostic dump into a restart.
"""

from __future__ import annotations

import faulthandler
import io
import signal
from unittest.mock import Mock

from gateway.run_startup import GatewayStartupMixin


def test_sigusr2_faulthandler_registration_does_not_chain(monkeypatch):
    sigusr2 = getattr(signal, "SIGUSR2", None)
    if sigusr2 is None:
        sigusr2 = Mock(name="SIGUSR2")
        monkeypatch.setattr(signal, "SIGUSR2", sigusr2, raising=False)
    dump_handle = io.StringIO()
    enable = Mock()
    register = Mock()
    monkeypatch.setattr(faulthandler, "enable", enable)
    monkeypatch.setattr(faulthandler, "register", register, raising=False)
    startup = GatewayStartupMixin()
    monkeypatch.setattr(startup, "_open_faulthandler_log", lambda: dump_handle)

    startup._start_install_faulthandler()

    enable.assert_called_once_with()
    register.assert_called_once_with(sigusr2, file=dump_handle, all_threads=True, chain=False)
