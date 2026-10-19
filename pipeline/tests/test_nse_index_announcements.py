"""NSE Indices' press releases: the parser that dates index membership changes.

The fixtures are short excerpts of real releases as pypdf reads them (spacing
quirks included: "Sub -Committee", "above -mentioned"), cut to the lines the
parser needs. Where a release is quoted, its id is named.
"""

# The fixtures keep NSE's own line breaks, which run past the line limit.
# ruff: noqa: E501
from __future__ import annotations

from datetime import date

import pytest

from pipeline.sources import nse_index_announcements as a


def _release(ref: str, title: str, announced: date, wef: date | None = None) -> a.PressRelease:
    return a.PressRelease(ref=ref, announced_on=announced, title=title,
                          url=f"https://niftyindices.com/Press_Release/{ref}.pdf", wef=wef)


REVIEW = _release("ind_prs10082026", "Replacements in indices w.e.f. September 30, 2026",
                  date(2026, 8, 10), date(2026, 9, 30))

# ind_prs10082026, page 1 (Nifty 50) and the start of page 2 (Nifty 500).
REVIEW_TEXT = """
PRESS RELEASE
Mumbai, August 10, 2026
Replacements in indices

The Index Maintenance Sub -Committee (Equity) of NSE Indices Limited has decided to make
replacement of stocks in various indices as part of its periodic review as listed hereunder. These changes
shall become effective from September 30, 2026 (close of September 29, 2026).

A. Replacements on account of semi-annual review of broad market indices:

1) Nifty 50

The following company is being excluded:

Sr. No. Company Name Symbol
1 Wipro Ltd. WIPRO

The following company is being included:

Sr. No. Company Name Symbol
1 BSE Ltd. BSE

Note:
1. BSE Ltd. (average free-float market capitalization Rs. 1,40,879 crores) has been included in Nifty 50 index
2. Next two eligible companies namely TVS Motor Company Ltd. (Rs. 84,566 crores) and Divi's Laboratories
3. Stocks that are available for trading in NSE's Futures & Options segment are only eligible for inclusion in
the index.

The above replacement will also be applicable to Nifty50 Equal Weight index.

2) Nifty 500

The following companies are being excluded:

Sr. No. Company Name Symbol
1 3M India Ltd. 3MINDIA
2 Aditya Birla Fashion and Retail Ltd. ABFRL
Sr. No. Company Name Symbol
3 Aditya Birla Lifestyle Brands Ltd. ABLBL

The following companies are being included:

Sr. No. Company Name Symbol
1 Aether Industries Ltd. AETHER
2 Avanti Feeds Ltd. AVANTIFEED
3 Azad Engineering Ltd. AZAD

About NSE Indices Limited:
NSE Indices Limited (formerly known as India Index Services & Products Ltd. - IISL), a subsidiary of
1 Not A Member Ltd. NOTAMEMBER
"""


def _by(changes: list[a.Change], index: str) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {a.INCLUDE: set(), a.EXCLUDE: set()}
    for c in changes:
        if c.index_name == index:
            out[c.action].add(c.symbol)
    return out


def test_a_review_section_reads_the_swap_and_nse_s_effective_date():
    pr = a.parse_release_text(REVIEW_TEXT, REVIEW)
    assert not pr.unparsed
    assert _by(pr.changes, "Nifty 50") == {a.EXCLUDE: {"WIPRO"}, a.INCLUDE: {"BSE"}}
    # "effective from September 30 (close of September 29)": the session it
    # counts from is the 30th, never the "close of" date.
    assert {c.effective_from for c in pr.changes} == {date(2026, 9, 30)}
    wipro = next(c for c in pr.changes if c.symbol == "WIPRO")
    assert (wipro.ref, wipro.announced_on, wipro.company) == (
        "ind_prs10082026", date(2026, 8, 10), "Wipro Ltd.")


def test_a_table_split_by_a_page_break_keeps_its_section_and_the_footer_is_not_read():
    pr = a.parse_release_text(REVIEW_TEXT, REVIEW)
    assert _by(pr.changes, "Nifty 500") == {
        a.EXCLUDE: {"3MINDIA", "ABFRL", "ABLBL"}, a.INCLUDE: {"AETHER", "AVANTIFEED", "AZAD"}}
    assert "NOTAMEMBER" not in {c.symbol for c in pr.changes}
    # The notes under the Nifty 50 table ("1. BSE Ltd. ...") are not rows.
    assert len(_by(pr.changes, "Nifty 50")[a.INCLUDE]) == 1


def test_also_applicable_copies_the_section_s_changes_to_the_named_index():
    pr = a.parse_release_text(REVIEW_TEXT, REVIEW)
    assert _by(pr.changes, "Nifty50 Equal Weight") == {a.EXCLUDE: {"WIPRO"}, a.INCLUDE: {"BSE"}}
    # Only the Nifty 50 section's changes: Nifty 500's come after it.
    assert len([c for c in pr.changes if c.index_name == "Nifty50 Equal Weight"]) == 2


def test_a_lettered_heading_that_names_one_index_gives_its_rows_that_index():
    # ind_prs15092026 section B: "Nifty50" has no space, which a word
    # boundary after "Nifty" would miss.
    rel = _release("ind_prs15092026", "Replacements in indices w.e.f. September 30, 2026",
                   date(2026, 9, 15), date(2026, 9, 30))
    text = """
These changes shall become
effective from September 30, 2026 (close of September 29, 2026).
B. Replacement in Nifty50 Value 20 index:

The following company is being excluded on account of exclusion from Nifty 50:

Sr. No. Company Name Symbol
1 Wipro Ltd. WIPRO

The following company is being included:

Sr. No. Company Name Symbol
1 Dr. Reddy's Laboratories Ltd. DRREDDY
"""
    pr = a.parse_release_text(text, rel)
    assert not pr.unparsed
    assert _by(pr.changes, "Nifty50 Value 20") == {a.EXCLUDE: {"WIPRO"}, a.INCLUDE: {"DRREDDY"}}


def test_a_single_index_release_takes_its_index_from_the_title():
    # ind_prs23092026_2: no "1)" heading anywhere in the text.
    rel = _release("ind_prs23092026_2", "Exclusion from Nifty SME Emerge index w.e.f. September 25, 2026",
                   date(2026, 9, 23), date(2026, 9, 25))
    text = """
Exclusion from Nifty SME Emerge index
NSE vide its circular NSE/CML/76509 announced migration of Parin Enterprises Ltd. from NSE's SME
Emerge platform to NSE's Capital Market segment (Main Board). In accordance with the circular, the
Index Maintenance Sub-Committee (Equity) of NSE Indices Limited has decided to make the following
exclusion from Nifty SME Emerge index. The change shall become effective from September 25, 2026
(close of September 24, 2026).
The following company is being excluded:
Sr. No. Company Name Symbol
1 Parin Enterprises Ltd. PARIN
No inclusion is being made in the index.
"""
    pr = a.parse_release_text(text, rel)
    assert [(c.index_name, c.action, c.symbol, c.effective_from) for c in pr.changes] == [
        ("Nifty SME Emerge", a.EXCLUDE, "PARIN", date(2026, 9, 25))]


def test_one_company_out_of_an_index_table_carries_the_company_not_a_symbol():
    # ind_prs20072026: the stock's NSE symbol is printed nowhere ("AGL" is an
    # abbreviation), so the change names the company and the builder matches it.
    rel = _release("ind_prs20072026",
                   "Exclusion of Allcargo Global Ltd. from Nifty Indices w.e.f. July 22, 2026",
                   date(2026, 7, 20), date(2026, 7, 22))
    text = """
Exclusion of Allcargo Global Ltd. from Nifty Indices
NSE Indices Limit ed vide its press release dated November 07, 202 5 had announced inclusion of
Allcargo Global Ltd. (AGL), with dummy symbol " DUMMYALCAR" on account of demerger of
International Supply Chain business from Allcargo Logistics Ltd. in various indices effective from
November 12, 2025 (close of November 11, 2025). After the demerger, Allcargo Global Ltd. was listed
on July 03, 2026 at the National Stock Exchange of India Ltd. (NSE).
Sub-Committee (Equity) of NSE Indices Ltd. has decided to exclude the company from various indices
as listed hereunder effective from July 22, 2026 (close of July 21, 2026).
Sr. No. Index Name
1 Nifty Microcap 250
2 Nifty Total Market
"""
    pr = a.parse_release_text(text, rel)
    assert not pr.unparsed
    assert [(c.index_name, c.action, c.symbol, c.company, c.effective_from) for c in pr.changes] == [
        ("Nifty Microcap 250", a.EXCLUDE, "", "Allcargo Global Ltd.", date(2026, 7, 22)),
        ("Nifty Total Market", a.EXCLUDE, "", "Allcargo Global Ltd.", date(2026, 7, 22)),
    ]


def test_a_date_revision_names_the_symbol_its_index_and_both_dates():
    # ind_prs16092025.
    rel = _release("ind_prs16092025", "Revision in exclusion date for Nifty SME Emerge index",
                   date(2025, 9, 16))
    text = """
Revision in exclusion date for Nifty SME Emerge index
The Index Maintenance Sub Committee (Equity) of NSE Indices Limited via press release dated
September 12, 2025 had announced exclusion of Mangalam Worldwide Ltd. (MWL) from Nifty SME
Emerge index with effect from September 19, 2025 (close of September 18, 2025).
In view of the above, the Index Maintenance Sub-Committee (Equity) has decided to revise the date of
exclusion of the above -mentioned security from September 19, 2025 to September 18, 2025 (close of
September 17, 2025).
"""
    pr = a.parse_release_text(text, rel)
    assert not pr.unparsed
    assert pr.revisions == [a.Revision("ind_prs16092025", date(2025, 9, 16), a.EXCLUDE, "MWL",
                                       "Nifty SME Emerge", date(2025, 9, 19), date(2025, 9, 18))]


def test_each_section_of_a_two_date_release_keeps_its_own_effective_date():
    # The shape of ind_prs23042026 (an "A." and a "B." section, each with its
    # own "effective from"), with the dates made different.
    rel = _release("ind_prs01012027", "Replacements in indices", date(2027, 1, 1))
    text = """
A. Replacement on account of corporate action
These changes shall become effective from January 08, 2027 (close of January 07, 2027).
1) Nifty Auto
The following company is being excluded:
Sr. No. Company Name Symbol
1 Old Motors Ltd. OLDMOTOR
B. Replacement on account of monthly review of Nifty Shariah indices:
The changes shall become effective from
January 29, 2027 (close of January 28, 2027).
1) Nifty500 Shariah
The following company is being included:
Sr. No. Company Name Symbol
1 Acutaas Chemicals Ltd. ACUTAAS
"""
    pr = a.parse_release_text(text, rel)
    assert {(c.symbol, c.effective_from) for c in pr.changes} == {
        ("OLDMOTOR", date(2027, 1, 8)), ("ACUTAAS", date(2027, 1, 29))}


def test_a_membership_release_no_layout_reads_is_unparsed():
    rel = _release("ind_prs02022027", "Replacements in indices w.e.f. February 10, 2027",
                   date(2027, 2, 2), date(2027, 2, 10))
    prose = "The Sub-Committee has decided that Foo Ltd. shall replace Bar Ltd. in Nifty Bar."
    assert a.parse_release_text(prose, rel).unparsed
    # A table row with no index to give it to makes the release unparsed,
    # even beside a section that read fine: a shape was missed.
    orphan = """
These changes shall become effective from February 10, 2027.
The following company is being included:
Sr. No. Company Name Symbol
1 Foo Ltd. FOO
1) Nifty Bank
The following company is being excluded:
Sr. No. Company Name Symbol
1 Bar Bank Ltd. BARBANK
"""
    pr = a.parse_release_text(orphan, rel)
    assert [c.symbol for c in pr.changes] == ["BARBANK"]
    assert pr.orphan_rows == ["1 Foo Ltd. FOO"]
    assert pr.unparsed
    # A release that says in words that nothing changes has been read.
    none = "No changes are being made to Nifty India Railways PSU and Nifty Sugar & Ethanol indices."
    assert not a.parse_release_text(none, rel).unparsed


# ── the press-release index and the window ─────────────────────────────────

INDEX_PAGE = """
<div class="contentBlock">
  <div class="pressItem" data-date="Oct 14, 2026" hidden="true">
    <p>Oct 14, 2026</p>
    <a href='/Press_Release/ind_prs14102026.pdf' target="_blank">Changes in Nifty Fixed Income indices w.e.f. October 19, 2026</a>
  </div>
  <div class="pressItem" data-date="Oct 12, 2026" hidden="true">
    <p>Oct 12, 2026</p>
    <a href='/Press_Release/ind_prs12102026.pdf' target="_blank">Replacement in indices w.e.f. October 13, 2026</a>
  </div>
  <div class="pressItem" data-date="Sep 11, 2026" hidden="true">
    <p>Sep 11, 2026</p>
    <a href='/Press_Release/ind_prs11092026_1.pdf' target="_blank">Higher frequency for real time index dissemination of Nifty 50</a>
  </div>
  <div class="pressItem" data-date="Aug 10, 2026" hidden="true">
    <p>Aug 10, 2026</p>
    <a href='/Press_Release/ind_prs10082026.pdf' target="_blank">Replacements in indices w.e.f. September 30, 2026</a>
  </div>
  <div class="pressItem" data-date="Aug 10, 2026" hidden="true">
    <p>Aug 10, 2026</p>
    <a href='/Press_Release/ind_prs10082026_2.pdf' target="_blank">Exclusion from Nifty SME Emerge index w.e.f. August 17, 2026</a>
  </div>
  <div class="pressItem" data-date="Sep 16, 2025" hidden="true">
    <p>Sep 16, 2025</p>
    <a href='/Press_Release/ind_prs16092025.pdf' target="_blank">Revision in exclusion date for Nifty SME Emerge index</a>
  </div>
  <div class="pressItem" data-date="Feb 18, 2025" hidden="true">
    <p>Feb 18, 2025</p>
    <a href='/Press_Release/ind_prs18022025.pdf' target="_blank">Replacements in indices w.e.f. February 28, 2028</a>
  </div>
  <div class="pressItem" data-date="Jun 15, 2026" hidden="true">
    <p>Jun 15, 2026</p>
    <a href='/Press_Release/ind_prs15062026_1' target="_blank">Inclusion in Nifty IPO w.e.f. June 18, 2026</a>
  </div>
</div>
"""


def test_the_press_release_index_lists_each_release_with_its_title_date():
    rels = {r.ref: r for r in a.parse_press_release_index(INDEX_PAGE)}
    r = rels["ind_prs10082026"]
    assert (r.announced_on, r.wef, r.url) == (
        date(2026, 8, 10), date(2026, 9, 30),
        "https://niftyindices.com/Press_Release/ind_prs10082026.pdf")
    assert rels["ind_prs16092025"].wef is None
    # NSE sometimes links a release without ".pdf".
    assert rels["ind_prs15062026_1"].url.endswith("/ind_prs15062026_1")


def test_the_window_reads_membership_releases_that_can_still_matter():
    rels = a.parse_press_release_index(INDEX_PAGE)
    picked = [r.ref for r in a.select_releases(rels, date(2026, 10, 5), grace_days=10,
                                               lookback_days=120)]
    # 09-30 is within 10 days of 10-05. Fixed income, dissemination, a past
    # SME exclusion (08-17), a revision announced over 120 days ago and a
    # typo'd "2028" from 2025 are not read; 10-12's release did not exist yet.
    assert picked == ["ind_prs10082026"]
    # The grace window's edge: 09-30 is read through 10-10, not on 10-11.
    edge = [r.ref for r in a.select_releases(rels, date(2026, 10, 10), grace_days=10,
                                             lookback_days=120)]
    assert edge == ["ind_prs10082026"]
    past = [r.ref for r in a.select_releases(rels, date(2026, 10, 11), grace_days=10,
                                             lookback_days=120)]
    assert past == []
    later = [r.ref for r in a.select_releases(rels, date(2026, 10, 12), grace_days=10,
                                              lookback_days=120)]
    assert later == ["ind_prs12102026"]


# ── pdf_text ────────────────────────────────────────────────────────────────

def _tiny_pdf(lines: list[str]) -> bytes:
    """A one-page PDF whose text is `lines`, with a correct xref."""
    def esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    shown = " ".join(f"({esc(ln)}) Tj T*" for ln in lines)
    stream = f"BT /F1 10 Tf 40 800 Td 12 TL {shown} ET"
    objs = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Contents 4 0 R "
        "/Resources << /Font << /F1 5 0 R >> >> >>",
        f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = b"%PDF-1.4\n"
    offsets = []
    for i, body in enumerate(objs, 1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n{body}\nendobj\n".encode()
    xref = len(out)
    out += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode()
    out += b"".join(f"{o:010d} 00000 n \n".encode() for o in offsets)
    out += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return out


def test_pdf_text_reads_a_release_and_rejects_an_html_page():
    text = a.pdf_text(_tiny_pdf(["1) Nifty 50", "1 Wipro Ltd. WIPRO"]))
    assert "Nifty 50" in text and "WIPRO" in text
    with pytest.raises(a.MalformedAnnouncement):
        a.pdf_text(b"<!DOCTYPE html><html>Service unavailable</html>")
