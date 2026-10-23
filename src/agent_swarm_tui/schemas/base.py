"""Base contract model shared by every schema in the swarm."""

from pydantic import BaseModel, ConfigDict


class StrictModel(BaseModel):
    """Immutable, closed, non-coercing contract.

    - ``extra="forbid"``: unknown fields are rejected.
    - ``frozen=True``: instances are immutable (and hashable).
    - ``strict=True``: no implicit type coercion (``"1"`` is not an ``int``).

    Strict mode still accepts the canonical JSON encodings (UUID, datetime and
    enum values as strings) via ``model_validate_json``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
