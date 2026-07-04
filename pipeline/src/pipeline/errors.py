"""Typed pipeline errors so callers can branch on expected vs unexpected."""
from __future__ import annotations


class PipelineError(Exception):
    """Base for all pipeline errors."""


class NotYetPublished(PipelineError):
    """The bhavcopy for this date is not available yet (expected window)."""


class UnexpectedFailure(PipelineError):
    """An unexpected failure (format break, blocked, exhausted fallbacks)."""
