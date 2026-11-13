"""Broker funds stub. Live getRMS sync arrives with the Broker Bridge (DECISION.MD B.2)."""

from __future__ import annotations

from typing import Optional


def get_available_funds() -> Optional[float]:
    """Return live available cash from the broker, or None when unknown (stub)."""
    return None
