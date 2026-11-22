"""Data-source clients. Each returns a tidy pandas DataFrame and never raises on
an empty/absent key — it returns an empty frame with the right schema so the
Bronze layer can record a zero-row, auditable ingestion."""

from .config import load_sources
from .fred import FredClient
from .world_bank import WorldBankClient
from .imf import ImfClient
from .yahoo import MarketClient
from .edgar import EdgarClient

__all__ = [
    "load_sources",
    "FredClient",
    "WorldBankClient",
    "ImfClient",
    "MarketClient",
    "EdgarClient",
]
