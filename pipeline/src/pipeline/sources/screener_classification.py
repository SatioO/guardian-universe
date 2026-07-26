"""Pure Screener classification parsing for the Classification Registry.

The collector owns HTTP, pacing, and registry state. This module only turns a
single saved company page into a complete, canonical taxonomy observation so it
can be fixture-tested without network access.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from html.parser import HTMLParser


def canonicalize_label(value: str) -> str:
    """Return the comparison-safe form of a source taxonomy label."""
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value)).strip()


@dataclass(frozen=True)
class ClassificationObservation:
    """A complete four-tier classification extracted from one source page."""

    macro_sector: str
    sector: str
    industry: str
    basic_industry: str


_TITLE_TO_FIELD = {
    "Broad Sector": "macro_sector",
    "Sector": "sector",
    "Broad Industry": "industry",
    "Industry": "basic_industry",
}


class _PeerTitlesParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.values: dict[str, str] = {}
        self.inconsistent = False
        self._active_field: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a" or self._active_field is not None:
            return
        title = dict(attrs).get("title")
        field = _TITLE_TO_FIELD.get(title) if title is not None else None
        if field is not None:
            self._active_field = field
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._active_field is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or self._active_field is None:
            return
        value = canonicalize_label("".join(self._text))
        if value:
            prior = self.values.get(self._active_field)
            if prior is not None and prior != value:
                self.inconsistent = True
            else:
                self.values[self._active_field] = value
        self._active_field = None
        self._text = []


def parse_screener_classification(page_html: str) -> ClassificationObservation | None:
    """Return a complete taxonomy observation, or ``None`` for partial pages."""
    parser = _PeerTitlesParser()
    parser.feed(page_html)
    parser.close()
    if parser.inconsistent:
        return None
    try:
        return ClassificationObservation(**{
            field: parser.values[field]
            for field in ("macro_sector", "sector", "industry", "basic_industry")
        })
    except KeyError:
        return None
