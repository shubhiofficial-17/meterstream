from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    broker: str = field(default_factory=lambda: _env("BROKER", "memory"))  # memory | redis
    redis_url: str = field(default_factory=lambda: _env("REDIS_URL", "redis://localhost:6379/0"))
    store: str = field(default_factory=lambda: _env("STORE", "memory"))  # memory | timescale
    database_url: str = field(
        default_factory=lambda: _env("DATABASE_URL", "postgresql://meter:meter@localhost:5432/meter")
    )
    partitions: int = field(default_factory=lambda: int(_env("PARTITIONS", "1")))
    partition: int = field(default_factory=lambda: int(_env("PARTITION", "0")))  # which one this worker owns
    interval_s: int = field(default_factory=lambda: int(_env("INTERVAL_S", "300")))
    # Comma-separated meter IDs registered with generation (in production: from the asset registry).
    solar_meters_file: str = field(default_factory=lambda: _env("SOLAR_METERS_FILE", ""))


def load_solar_meters(path: str) -> set[str]:
    if not path or not os.path.exists(path):
        return set()
    with open(path) as f:
        return {line.strip() for line in f if line.strip()}


def build_broker(s: Settings):
    if s.broker == "redis":
        from meterstream.transport.redis_streams import RedisBroker

        return RedisBroker(s.redis_url, partitions=s.partitions, partition=s.partition)
    from meterstream.transport.memory import MemoryBroker

    return MemoryBroker()


def build_store(s: Settings):
    if s.store == "timescale":
        from meterstream.storage.timescale import TimescaleStore

        return TimescaleStore(s.database_url)
    from meterstream.storage.memory import MemoryStore

    return MemoryStore()
