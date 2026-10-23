"""Environment-driven gateway client configuration (pydantic-settings).

Every option reads ``GATEWAY_<FIELD>`` (e.g. ``GATEWAY_BASE_URL``,
``GATEWAY_READ_TIMEOUT_S``, ``GATEWAY_AUTH_TOKEN``). Unknown ``GATEWAY_*``
variables, such as the gateway server's own settings, are ignored.
"""

import ipaddress
from typing import Self

from pydantic import Field, HttpUrl, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_LOOPBACK_NAMES = frozenset({"localhost", "localhost.localdomain"})


class GatewayClientSettings(BaseSettings):
    """Connection settings for ai-inference-gateway; immutable once loaded."""

    model_config = SettingsConfigDict(env_prefix="GATEWAY_", frozen=True, extra="forbid")

    base_url: HttpUrl = Field(default=HttpUrl("http://127.0.0.1:8000"))
    connect_timeout_s: float = Field(default=5.0, gt=0.0, le=120.0)
    read_timeout_s: float = Field(
        default=120.0,
        gt=0.0,
        le=900.0,
        description="Maximum silence between SSE chunks (includes time to first token).",
    )
    write_timeout_s: float = Field(default=10.0, gt=0.0, le=120.0)
    pool_timeout_s: float = Field(default=10.0, gt=0.0, le=120.0)
    stream_timeout_s: float = Field(
        default=300.0, gt=0.0, le=3600.0, description="Overall deadline for one request."
    )
    max_connections: int = Field(default=64, ge=1, le=1024)
    auth_token: SecretStr | None = Field(
        default=None, description="Sent as 'Authorization: Bearer <token>' (e.g. for a proxy)."
    )
    trust_env: bool = Field(
        default=False, description="Honour HTTP(S)_PROXY / NO_PROXY / .netrc from the environment."
    )

    @model_validator(mode="after")
    def _url_is_safe(self) -> Self:
        url = self.base_url
        if url.username or url.password:
            raise ValueError("base_url must not embed credentials; use GATEWAY_AUTH_TOKEN")
        if url.query or url.fragment:
            raise ValueError("base_url must not contain a query string or fragment")
        if self.auth_token is not None and url.scheme == "http" and not _is_loopback(url.host):
            raise ValueError("refusing to send GATEWAY_AUTH_TOKEN over plain http to a remote host")
        return self


def _is_loopback(host: str | None) -> bool:
    if host is None:
        return False
    if host.lower() in _LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        return False
