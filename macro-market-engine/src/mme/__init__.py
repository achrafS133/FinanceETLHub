"""Macro-to-Market Engine reusable core.

Pure-Python, unit-testable building blocks shared by the Fabric notebooks:

- ``mme.ingestion`` : thin clients for FRED, World Bank, IMF, Yahoo, SEC EDGAR.
- ``mme.common``    : financial time-series utilities (calendar, transforms,
                      as-of joins, idempotent Delta merge helpers).

Inside Fabric, upload this package as a custom library / environment, or copy
``src/mme`` into the notebook's built-in Resources folder and ``%run`` / import it.
"""

__version__ = "0.1.0"
