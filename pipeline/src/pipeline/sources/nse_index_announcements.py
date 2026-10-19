"""NSE Indices' press releases: WHEN an index's membership changes.

Pure parse — no network, no I/O — so it is unit-testable in isolation. The
fetch and the application to the published list live in
`builders.build_index_constituents`.

NSE's per-index CSVs (`nse_constituents`) say who is in an index. They say
nothing about when a change takes effect, and NSE posts them on its own
schedule: the 09-29 build still listed WIPRO in Nifty 50 at 22:09 IST, the
evening before the 2026-09-30 review took effect. The press releases carry the
effective date ("These changes shall become effective from September 30, 2026
(close of September 29, 2026)") and, per index, the symbols going out and in.

Two sources on niftyindices.com:
  - the press-release index (`/press-release`): one HTML page listing every
    release with its date, PDF and title. Most membership titles carry
    "w.e.f. <date>", which is what selects the few PDFs a run reads.
  - each release's PDF, read to text with pypdf.

Layouts the parser reads (all seen in 2025-26 releases):
  1. "N) <Index>" sections, each with "being excluded" / "being included"
     tables of "Sr. No. Company Name Symbol". Also a lettered heading that
     names one index ("B. Replacement in Nifty50 Value 20 index:"), and "The
     above ... also applicable to <Index> index", which copies the section's
     changes to that index.
  2. A single-index release with no section heading: the index comes from the
     title ("Exclusion from Nifty SME Emerge index w.e.f. ...").
  3. One company out of many indices: "Exclusion of <Company> from Nifty
     Indices", with a "Sr. No. Index Name" table. No symbol is printed, so the
     change carries the company name and the builder matches it to a member.
  4. A date revision: "revise the date of exclusion of the above-mentioned
     security from D1 to D2", which moves that symbol's announced change.

A membership release that yields none of these is UNPARSED. The builder turns
that into a loud failure naming the PDF; a human records how to read it in
`seeds/index_change_overrides.csv`. Nothing is skipped silently.
"""

from __future__ import annotations

import html as _html
import io
import re
from dataclasses import dataclass, field
from datetime import date, timedelta

PRESS_RELEASE_INDEX_URL = "https://niftyindices.com/press-release"
BASE_URL = "https://niftyindices.com"

INCLUDE = "include"
EXCLUDE = "exclude"

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
# "September 30, 2026", "Sep 30, 2026", "July 01 2026", "Aug 24, 2026". pypdf
# sometimes splits a year ("202 5"), so the year tolerates one inner space.
_DATE = r"([A-Z][a-z]{2,8})\.?\s+(\d{1,2}),?\s+(\d\s?\d\s?\d\s?\d)"
_DATE_RE = re.compile(_DATE)
_EFFECTIVE_RE = re.compile(r"(?:effective|with\s+effect)\s+from\s+" + _DATE, re.I)
_WEF_RE = re.compile(r"w\.?\s?e\.?\s?f\.?\s+" + _DATE, re.I)

_MEMBERSHIP_TITLE_RE = re.compile(r"\b(replacements?|inclusions?|exclusions?)\b", re.I)
# Not equity membership: NSE's fixed-income family uses the same verbs.
_NON_EQUITY_TITLE_RE = re.compile(
    r"g-?sec|bond|sdl|t-?bill|fixed income|money market|debt|treasury|commercial paper",
    re.I,
)
# A title that names exactly one index ("Inclusion in Nifty IPO w.e.f. ...").
_TITLE_INDEX_RE = re.compile(
    r"^(?:replacements?|inclusions?|exclusions?)\s+(?:in|from)\s+(nifty.+?)"
    r"(?:\s+index)?(?:\s+w\.?\s?e\.?\s?f\..*)?$",
    re.I,
)
_TITLE_COMPANY_EXCLUSION_RE = re.compile(r"^exclusion of (.+?) from nifty indices", re.I)

_SECTION_RE = re.compile(r"^\s*(\d{1,2})\)\s+(.+?)\s*$")
_LETTER_RE = re.compile(r"^\s*([A-H])\.\s+(.+?)\s*$")
_LETTER_INDEX_RE = re.compile(
    r"^(?:replacements?|inclusions?|exclusions?)\s+in\s+(nifty.+?)(?:\s+index)?\s*:?\s*$", re.I
)
_ACTION_RE = re.compile(
    r"following\s+compan(?:y|ies)\s+(?:is|are)\s+being\s+(excluded|included)", re.I
)
_COMPANY_TABLE_RE = re.compile(r"^\s*Sr\.?\s*No\.?\s+Company(?:\s+Name)?\s+Symbol\s*$", re.I)
_INDEX_TABLE_RE = re.compile(r"^\s*Sr\.?\s*No\.?\s+Index\s+Name\s*$", re.I)
_ROW_RE = re.compile(r"^\s*(\d{1,3})\s+(.+?)\s+([A-Z0-9][A-Z0-9&\-]*)\s*$")
_INDEX_ROW_RE = re.compile(r"^\s*(\d{1,3})\s+(nifty.+?)\s*$", re.I)
_ALSO_RE = re.compile(
    r"(?:above|these)\s+(?:replacements?|changes?|exclusions?|inclusions?)\s+(?:will\s+)?"
    r"(?:also\s+be|be\s+also|are\s+also|is\s+also|shall\s+also\s+be)\s+applicable\s+"
    r"(?:to|for)\s+(?:the\s+)?(.+?)\s+ind(?:ex|ices)\b",
    re.I,
)
_REVISION_RE = re.compile(
    r"revise\s+the\s+date\s+of\s+(inclusion|exclusion)\s+of\s+.+?\s+from\s+"
    + _DATE + r"\s+to\s+" + _DATE,
    re.I,
)
_REVISION_SUBJECT_RE = re.compile(
    r"announced\s+(?:inclusion|exclusion)\s+of\s+.+?\(([A-Z0-9][A-Z0-9&\-]*)\)\s+"
    r"(?:in|from|into)\s+(nifty.+?)\s+index",
    re.I,
)
# The boilerplate every release ends with; nothing after it is membership.
_FOOTER_RE = re.compile(r"^\s*About NSE Indices Limited", re.I)


class MalformedAnnouncement(ValueError):
    """The payload is not a readable press release."""


def company_key(name: str) -> str:
    """A company name's comparison form: "Allcargo Global Ltd." and
    "ALLCARGO GLOBAL LIMITED" compare equal."""
    s = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    s = re.sub(r"\b(?:ltd|limited|the)\b", " ", s)
    return "".join(s.split())


@dataclass(frozen=True)
class PressRelease:
    ref: str              # "ind_prs10082026": the PDF's basename, NSE's own id
    announced_on: date
    title: str
    url: str
    wef: date | None      # the title's "w.e.f." date, when it has one


@dataclass(frozen=True)
class Change:
    ref: str
    seq: int              # order inside the release; a tie-break, nothing more
    announced_on: date
    effective_from: date
    index_name: str       # NSE's spelling in the release
    action: str           # INCLUDE | EXCLUDE
    symbol: str           # "" when the release names only the company (layout 3)
    company: str


@dataclass(frozen=True)
class Revision:
    ref: str
    announced_on: date
    action: str
    symbol: str
    index_name: str | None
    old: date
    new: date


@dataclass
class ParsedRelease:
    release: PressRelease
    changes: list[Change] = field(default_factory=list)
    revisions: list[Revision] = field(default_factory=list)
    # True when the release says, in words, that nothing changes in a named
    # index ("No changes are being made to ..."); such a release is read.
    states_no_change: bool = False
    # Table rows found with no index to give them to: the shape was not read.
    orphan_rows: list[str] = field(default_factory=list)

    @property
    def unparsed(self) -> bool:
        if self.orphan_rows:
            return True
        return not (self.changes or self.revisions or self.states_no_change)


def parse_date(month: str, day: str, year: str) -> date:
    m = _MONTHS.get(month[:3].lower())
    if m is None:
        raise ValueError(f"not a month: {month!r}")
    return date(int(year.replace(" ", "")), m, int(day))


def _first_date(text: str) -> date | None:
    m = _DATE_RE.search(text)
    if not m:
        return None
    try:
        return parse_date(*m.groups())
    except ValueError:
        return None


# ── the press-release index ────────────────────────────────────────────────

_ITEM_RE = re.compile(
    r'data-date="([^"]+)"[^<]*<p>[^<]*</p>\s*<a\s+href=[\'"]([^\'"]+)[\'"][^>]*>([^<]*)</a>',
    re.S,
)


def parse_press_release_index(page: str) -> list[PressRelease]:
    """Every release on niftyindices.com/press-release, newest first as listed."""
    out: list[PressRelease] = []
    for raw_date, href, raw_title in _ITEM_RE.findall(page):
        announced = _first_date(raw_date)
        if announced is None:
            continue
        title = " ".join(_html.unescape(raw_title).split())
        path = href.strip()
        if not path.startswith(("http", "/")):
            path = "/" + path
        url = path if path.startswith("http") else f"{BASE_URL}{path}"
        ref = path.rsplit("/", 1)[-1].removesuffix(".pdf")
        wef_m = _WEF_RE.search(title)
        wef = None
        if wef_m:
            try:
                wef = parse_date(*wef_m.groups())
            except ValueError:
                wef = None
        out.append(PressRelease(ref=ref, announced_on=announced, title=title, url=url, wef=wef))
    return out


def is_membership_title(title: str) -> bool:
    return bool(_MEMBERSHIP_TITLE_RE.search(title)) and not _NON_EQUITY_TITLE_RE.search(title)


def select_releases(
    releases: list[PressRelease], session: date, *, grace_days: int, lookback_days: int
) -> list[PressRelease]:
    """The membership releases that can still matter to session S's list.

    Every release read was announced within lookback_days of S (NSE
    announces at most about seven weeks ahead: 08-10 for the 09-30 review),
    and, when its title has a date, that date is on or after S - grace_days.
    The announcement bound also drops a title whose date is a typo
    (`ind_prs18022025`, announced 2025-02-18, says "w.e.f. February 28,
    2028"). Releases announced after S are never read: a back-dated run must
    not see the future.
    """
    picked: list[PressRelease] = []
    seen: set[str] = set()
    for r in releases:
        if r.ref in seen or r.announced_on > session or not is_membership_title(r.title):
            continue
        keep = r.announced_on >= session - timedelta(days=lookback_days)
        if r.wef is not None:
            keep = keep and r.wef >= session - timedelta(days=grace_days)
        if keep:
            seen.add(r.ref)
            picked.append(r)
    return picked


# ── one release ─────────────────────────────────────────────────────────────

def pdf_text(payload: bytes) -> str:
    """A release's text, page after page. Raises MalformedAnnouncement for a
    payload that is not a PDF (an HTML error page served with 200)."""
    if not payload.lstrip().startswith(b"%PDF"):
        raise MalformedAnnouncement("not a PDF")
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    try:
        reader = PdfReader(io.BytesIO(payload))
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    except (PdfReadError, ValueError, KeyError) as e:
        raise MalformedAnnouncement(f"unreadable PDF: {e}") from e


def _clean_index_name(name: str) -> str:
    name = " ".join(name.split()).strip(" :.")
    return re.sub(r"\s+index$", "", name, flags=re.I)


def _split_names(text: str) -> list[str]:
    parts = re.split(r",|\s+and\s+", text)
    return [_clean_index_name(p) for p in parts if p.strip()]


def parse_release_text(text: str, release: PressRelease) -> ParsedRelease:
    """Read one release's membership changes. See the module docstring."""
    out = ParsedRelease(release=release)
    lines = text.splitlines()
    cut = next((i for i, ln in enumerate(lines) if _FOOTER_RE.match(ln)), len(lines))
    lines = [ln.rstrip() for ln in lines[:cut]]

    # Each line's effective date is the last "effective from <date>" before
    # it, so a two-section release keeps each section's own date.
    flat_parts: list[str] = []
    offsets: list[int] = []
    pos = 0
    for ln in lines:
        offsets.append(pos)
        flat_parts.append(ln)
        pos += len(ln) + 1
    flat = " ".join(flat_parts)
    dates: list[tuple[int, date]] = []
    for m in _EFFECTIVE_RE.finditer(flat):
        try:
            dates.append((m.start(), parse_date(*m.groups())))
        except ValueError:
            continue

    def effective_at(i: int) -> date | None:
        best = None
        for start, d in dates:
            if start <= offsets[i] + len(lines[i]):
                best = d
        return best if best is not None else release.wef

    title_index = None
    tm = _TITLE_INDEX_RE.match(release.title)
    if tm and not re.search(r"\bindices\b", tm.group(1), re.I):
        title_index = _clean_index_name(tm.group(1))

    index: str | None = title_index
    action: str | None = None
    table: str | None = None          # "company" | "index"
    section_start = 0                 # first change of the current index section
    index_rows: list[str] = []
    seq = 0

    for i, ln in enumerate(lines):
        s = ln.strip()
        if not s:
            continue
        if re.search(r"no\s+changes?\s+(?:is|are)\s+being\s+made", s, re.I):
            out.states_no_change = True
        # "1) Nifty 50" is a section; a table row ("1 Wipro Ltd. WIPRO") has
        # no parenthesis, and a note ("1. BSE Ltd. ...") has a period.
        sec = _SECTION_RE.match(s)
        if sec:
            index = _clean_index_name(sec.group(2))
            action, table = None, None
            section_start = len(out.changes)
            continue
        if re.match(r"^(?:note\b|no\s+inclusion|no\s+exclusion)", s, re.I):
            table = None
            continue
        let = _LETTER_RE.match(s)
        if let:
            named = _LETTER_INDEX_RE.match(let.group(2))
            index = _clean_index_name(named.group(1)) if named else title_index
            action, table = None, None
            section_start = len(out.changes)
            continue
        am = _ACTION_RE.search(s)
        if am:
            action = INCLUDE if am.group(1).lower() == "included" else EXCLUDE
            table = None
            continue
        if _COMPANY_TABLE_RE.match(s):
            table = "company"
            continue
        if _INDEX_TABLE_RE.match(s):
            table = "index"
            continue
        also_src = s
        wraps = re.search(r"applicable", s, re.I) and not re.search(r"ind(?:ex|ices)\b", s, re.I)
        if wraps and i + 1 < len(lines):
            also_src = f"{s} {lines[i + 1].strip()}"
        also = _ALSO_RE.search(also_src)
        if also and index is not None:
            section = [c for c in out.changes[section_start:] if c.index_name == index]
            for name in _split_names(also.group(1)):
                for c in section:
                    seq += 1
                    out.changes.append(
                        Change(c.ref, seq, c.announced_on, c.effective_from, name, c.action,
                               c.symbol, c.company)
                    )
            table = None
            continue
        if table == "company":
            row = _ROW_RE.match(s)
            if row:
                eff = effective_at(i)
                if index is None or action is None or eff is None:
                    out.orphan_rows.append(s)
                    continue
                seq += 1
                out.changes.append(
                    Change(release.ref, seq, release.announced_on, eff, index, action,
                           row.group(3).upper(), " ".join(row.group(2).split()))
                )
                continue
        if table == "index":
            row = _INDEX_ROW_RE.match(s)
            if row:
                index_rows.append(_clean_index_name(row.group(2)))
                continue

    # Layout 3: one company out of every index in the "Index Name" table.
    if index_rows:
        cm = _TITLE_COMPANY_EXCLUSION_RE.match(release.title)
        eff = dates[-1][1] if dates else release.wef
        if cm and eff is not None:
            company = " ".join(cm.group(1).split())
            for name in index_rows:
                seq += 1
                out.changes.append(
                    Change(release.ref, seq, release.announced_on, eff, name, EXCLUDE, "", company)
                )
        else:
            out.orphan_rows.extend(index_rows)

    # Layout 4: a date revision of an earlier release's change.
    rev = _REVISION_RE.search(flat)
    if rev:
        subj = _REVISION_SUBJECT_RE.search(flat)
        g = rev.groups()
        try:
            dates_moved: tuple[date, date] | None = (parse_date(*g[1:4]), parse_date(*g[4:7]))
        except ValueError:
            dates_moved = None
        if subj and dates_moved:
            out.revisions.append(
                Revision(release.ref, release.announced_on,
                         INCLUDE if g[0].lower() == "inclusion" else EXCLUDE,
                         subj.group(1).upper(), _clean_index_name(subj.group(2)), *dates_moved)
            )
    return out


@dataclass
class Announcements:
    """What one run read from NSE's press releases."""

    changes: list[Change] = field(default_factory=list)
    revisions: list[Revision] = field(default_factory=list)
    read: list[str] = field(default_factory=list)          # refs parsed
    unparsed: list[str] = field(default_factory=list)      # refs no layout read
    unavailable: list[str] = field(default_factory=list)   # fetch failures, transient


def collect(parsed: list[ParsedRelease]) -> Announcements:
    out = Announcements()
    for pr in parsed:
        if pr.unparsed:
            out.unparsed.append(pr.release.ref)
        else:
            out.read.append(pr.release.ref)
        out.changes.extend(pr.changes)
        out.revisions.extend(pr.revisions)
    return out
