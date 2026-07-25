"""Configuration for BATON's local control-plane service."""

from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True)
class ServiceConfig:
    """Runtime configuration loaded from explicit values or environment."""

    database: Path = Path("state/baton.sqlite")
    host: str = "127.0.0.1"
    port: int = 8020
    api_token: str | None = None
    allowed_origins: tuple[str, ...] = ()
    static_dir: Path | None = None
    max_body_bytes: int = 1_048_576

    def validate(self) -> "ServiceConfig":
        if not self.host.strip():
            raise ValueError("BATON_HOST cannot be empty")
        if not 0 <= self.port <= 65535:
            raise ValueError("BATON_PORT must be between 0 and 65535")
        if self.max_body_bytes < 1024:
            raise ValueError("BATON_MAX_BODY_BYTES must be at least 1024")
        if not _is_loopback(self.host) and not self.api_token:
            raise ValueError(
                "BATON_API_TOKEN is required when BATON_HOST is not loopback"
            )
        return self

    @classmethod
    def from_env(cls) -> "ServiceConfig":
        static_value = os.environ.get("BATON_STATIC_DIR", "").strip()
        origins = tuple(
            item.strip()
            for item in os.environ.get("BATON_ALLOWED_ORIGINS", "").split(",")
            if item.strip()
        )
        token = os.environ.get("BATON_API_TOKEN", "").strip() or None
        return cls(
            database=Path(os.environ.get("BATON_DATABASE", "state/baton.sqlite")),
            host=os.environ.get("BATON_HOST", "127.0.0.1"),
            port=int(os.environ.get("BATON_PORT", "8020")),
            api_token=token,
            allowed_origins=origins,
            static_dir=Path(static_value) if static_value else None,
            max_body_bytes=int(
                os.environ.get("BATON_MAX_BODY_BYTES", "1048576")
            ),
        ).validate()

    @property
    def loopback(self) -> bool:
        return _is_loopback(self.host)
