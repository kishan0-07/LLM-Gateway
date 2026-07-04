from dataclasses import dataclass


@dataclass(frozen=True)
class Principal:
    tenant_id: int
    api_key_id: int