"""Data-source adapters. Each isolates one upstream's URL/format quirks.

This package is also the broker-source REGISTRATION aggregator: importing
`pipeline.sources` must, as a side effect, import every concrete
`RebuildSource` module so each self-registers into `pipeline.rebuild`'s
registry (`RebuildSource.register()` at the bottom of each module). Broker
names BELONG here -- this is source-specific territory, the one place that is
allowed to name a concrete broker. Nothing above this package (cli.py,
rebuild.py, or any shared/dispatch code) ever names a broker; they import this
package as a whole and resolve sources purely through
`rebuild.resolve()`/`rebuild.REBUILDERS` (open id strings), never a literal
broker name.
"""
from __future__ import annotations

# registration side-effects; add new broker sources here — nothing above this
# package ever names a broker.
from pipeline.sources import kite_rebuild as _kite_rebuild  # noqa: F401
