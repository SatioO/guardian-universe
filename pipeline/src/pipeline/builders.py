"""Registry of derived-dataset builders, keyed by DatasetSpec.key.

Populated by later G1b tasks (6: reference/instruments, 7: ca_flags). This
module intentionally imports daily_update.RunStatus and datasets.DatasetSpec
only for typing -- builders themselves must stay name-free (no hardcoded
dataset-key lookups); the CLI is the allowed edge that resolves specs by name
and passes them in.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import date

from pipeline.daily_update import RunStatus
from pipeline.datasets import DatasetSpec

BUILDERS: dict[str, Callable[[DatasetSpec, date], RunStatus]] = {}
