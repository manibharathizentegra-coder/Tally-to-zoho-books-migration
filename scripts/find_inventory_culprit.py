#!/usr/bin/env python3
"""
Find the "culprit" behind Inventory Asset Value total differences in Zoho exports.

This script works with plain .xlsx files (no pandas/openpyxl needed). It:
  - detects columns whose header contains "Inventory Asset Value" (Zoho valuation export)
  - sums those columns (all rows vs visible rows only)
  - compares against a user-provided Zoho displayed total and/or a manual sum
  - tries to find 1..4 rows whose asset sum matches the delta (useful for ~₹500-₹600 type mismatches)

Example:
  python scripts/find_inventory_culprit.py ^
    --zoho "C:\\path\\Inventory Valuation Summary.xlsx" ^
    --adjustment "C:\\path\\opening_1775578265.xlsx" ^
    --zoho-displayed-total 3864377.32 ^
    --manual-sum 3863785.33 ^
    --out inventory_culprit_report.txt
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
import zipfile
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Iterable, Optional

import xml.etree.ElementTree as ET


getcontext().prec = 28


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


_CELL_REF_RE = re.compile(r"^([A-Za-z]+)(\d+)$")


def _col_letters_to_number(letters: str) -> int:
    n = 0
    for ch in letters.upper():
        if not ("A" <= ch <= "Z"):
            continue
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n


def _get_cell_col(cell_ref: str) -> Optional[int]:
    m = _CELL_REF_RE.match(cell_ref or "")
    if not m:
        return None
    return _col_letters_to_number(m.group(1))


def _norm_header(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _parse_decimal(val: Optional[str]) -> Optional[Decimal]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None

    # Excel numeric values are usually already plain like "1234.56".
    # But some exports contain formatted text like "1,234.56" or "(1,234.56)".
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()

    s = s.replace(",", "")
    s = re.sub(r"[₹$€£]", "", s).strip()

    # remove any non-number trailing junk
    s = re.sub(r"[^0-9eE+\-\.]", "", s)
    if not s or s in {"+", "-", ".", "+.", "-."}:
        return None

    try:
        d = Decimal(s)
    except InvalidOperation:
        return None
    return -d if neg else d


def _read_zip_text(z: zipfile.ZipFile, path: str) -> Optional[str]:
    try:
        with z.open(path) as f:
            return f.read().decode("utf-8", errors="replace")
    except KeyError:
        return None


def _get_shared_strings(z: zipfile.ZipFile) -> list[str]:
    xml_text = _read_zip_text(z, "xl/sharedStrings.xml")
    if not xml_text:
        return []
    root = ET.fromstring(xml_text)
    out: list[str] = []
    for si in root.findall(".//{*}si"):
        # shared string can be plain <t> or rich text with multiple <r><t>
        parts = [t.text or "" for t in si.findall(".//{*}t")]
        out.append("".join(parts))
    return out


@dataclass(frozen=True)
class SheetInfo:
    name: str
    path: str  # e.g. xl/worksheets/sheet1.xml


def _get_sheets(z: zipfile.ZipFile) -> list[SheetInfo]:
    workbook_xml = _read_zip_text(z, "xl/workbook.xml")
    rels_xml = _read_zip_text(z, "xl/_rels/workbook.xml.rels")
    if not workbook_xml or not rels_xml:
        raise ValueError("Invalid xlsx: missing xl/workbook.xml or workbook.xml.rels")

    wb_root = ET.fromstring(workbook_xml)
    rels_root = ET.fromstring(rels_xml)

    rid_to_target: dict[str, str] = {}
    for rel in rels_root.findall(".//{*}Relationship"):
        rid = rel.attrib.get("Id") or ""
        target = rel.attrib.get("Target") or ""
        if rid and target:
            rid_to_target[rid] = target

    sheets: list[SheetInfo] = []
    for sh in wb_root.findall(".//{*}sheet"):
        name = sh.attrib.get("name") or ""
        rid = sh.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id") or ""
        target = rid_to_target.get(rid)
        if not name or not target:
            continue
        if target.startswith("/"):
            target = target.lstrip("/")
        else:
            target = f"xl/{target}"
        sheets.append(SheetInfo(name=name, path=target.replace("\\", "/")))
    return sheets


@dataclass
class ParsedRow:
    row_num: int
    hidden: bool
    ids: dict[str, str]
    asset_values: dict[int, Decimal]  # col -> value
    asset_sum: Decimal


@dataclass
class SheetAnalysis:
    file_label: str
    sheet_name: str
    sheet_path: str
    header_row: Optional[int] = None
    header_map: dict[int, str] = field(default_factory=dict)  # col -> header text
    asset_cols: list[int] = field(default_factory=list)
    id_cols: list[int] = field(default_factory=list)
    # keys are either int column numbers (e.g. 7) or "vis:<col>" strings for visible-only sums
    totals_by_col: dict[object, Decimal] = field(default_factory=dict)
    total_all_asset_cols: Decimal = Decimal("0")
    total_all_asset_cols_visible_only: Decimal = Decimal("0")
    rows: list[ParsedRow] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _looks_like_id_header(h: str) -> bool:
    nh = _norm_header(h)
    patterns = [
        r"\bitem\s*id\b",
        r"\bitem\s*name\b",
        r"\bitem\b",
        r"\bsku\b",
        r"\bproduct\b",
        r"\bwarehouse\b",
        r"\bgodown\b",
        r"\bbranch\b",
        r"\bcategory\b",
        r"\bbrand\b",
        r"\bbatch\b",
        r"\bserial\b",
    ]
    return any(re.search(p, nh) for p in patterns)


def _match_any_header(h: str, patterns: Iterable[re.Pattern[str]]) -> bool:
    nh = _norm_header(h)
    return any(p.search(nh) for p in patterns)


def _read_cell_value(cell: ET.Element, shared: list[str]) -> Optional[str]:
    t = cell.attrib.get("t")
    v = cell.find("./{*}v")
    if t == "s":
        if v is None or v.text is None:
            return None
        try:
            idx = int(v.text)
        except ValueError:
            return None
        if 0 <= idx < len(shared):
            return shared[idx]
        return None
    if t == "inlineStr":
        # <c t="inlineStr"><is><t>...</t></is></c>
        parts = [n.text or "" for n in cell.findall(".//{*}is//{*}t")]
        s = "".join(parts).strip()
        return s or None
    if t == "str":
        return (v.text or "").strip() if v is not None else None
    # default: numbers and untyped strings both show up in <v>
    return (v.text or "").strip() if v is not None else None


def analyze_worksheet(
    z: zipfile.ZipFile,
    shared: list[str],
    sheet: SheetInfo,
    *,
    file_label: str,
    header_search_rows: int,
    asset_header_patterns: list[re.Pattern[str]],
) -> SheetAnalysis:
    try:
        with z.open(sheet.path) as f:
            # iterparse gives us streaming rows
            context = ET.iterparse(f, events=("start", "end"))
            next(context)  # prime iterator / read root

            analysis = SheetAnalysis(
                file_label=file_label,
                sheet_name=sheet.name,
                sheet_path=sheet.path,
            )

            header_row: Optional[int] = None
            header_map: dict[int, str] = {}
            asset_cols: list[int] = []
            id_cols: list[int] = []

            for event, elem in context:
                if event != "end":
                    continue
                if _local_name(elem.tag) != "row":
                    continue

                r_text = elem.attrib.get("r") or ""
                try:
                    r_num = int(r_text)
                except ValueError:
                    r_num = 0

                hidden = (elem.attrib.get("hidden") == "1")

                cell_nodes = [c for c in list(elem) if _local_name(c.tag) == "c"]
                if not cell_nodes:
                    elem.clear()
                    continue

                row_vals: dict[int, str] = {}
                row_raw: dict[int, str] = {}
                for c in cell_nodes:
                    col = _get_cell_col(c.attrib.get("r") or "")
                    if not col:
                        continue
                    raw = _read_cell_value(c, shared)
                    if raw is None:
                        continue
                    row_raw[col] = raw
                    # only treat non-empty as row value
                    s = str(raw).strip()
                    if s:
                        row_vals[col] = s

                # header detection
                if header_row is None and 1 <= r_num <= header_search_rows:
                    possible_asset = []
                    possible_headers = {}
                    for col, s in row_vals.items():
                        possible_headers[col] = s
                        if _match_any_header(s, asset_header_patterns):
                            possible_asset.append(col)

                    if possible_asset:
                        header_row = r_num
                        header_map = possible_headers
                        asset_cols = sorted(set(possible_asset))
                        id_cols = sorted(
                            c
                            for c, h in header_map.items()
                            if c not in asset_cols and _looks_like_id_header(h)
                        )
                        # fallback: if nothing looked like an ID, keep first few non-asset headers
                        if not id_cols:
                            id_cols = sorted(c for c in header_map.keys() if c not in asset_cols)[:3]

                # data rows
                if header_row is not None and r_num > header_row and asset_cols:
                    asset_values: dict[int, Decimal] = {}
                    row_asset_sum = Decimal("0")
                    has_any_asset = False
                    for col in asset_cols:
                        raw = row_raw.get(col)
                        d = _parse_decimal(raw)
                        if d is None:
                            continue
                        has_any_asset = True
                        asset_values[col] = d
                        row_asset_sum += d
                        analysis.totals_by_col[col] = analysis.totals_by_col.get(col, Decimal("0")) + d
                        if not hidden:
                            # keep visible totals by col too, to compare manual sums done with filters
                            key = f"vis:{col}"
                            analysis.totals_by_col[key] = analysis.totals_by_col.get(key, Decimal("0")) + d

                    if has_any_asset:
                        ids: dict[str, str] = {}
                        for col in id_cols:
                            hdr = header_map.get(col, f"col{col}")
                            ids[hdr] = row_vals.get(col, "")
                        analysis.rows.append(
                            ParsedRow(
                                row_num=r_num,
                                hidden=hidden,
                                ids=ids,
                                asset_values=asset_values,
                                asset_sum=row_asset_sum,
                            )
                        )

                elem.clear()

            analysis.header_row = header_row
            analysis.header_map = header_map
            analysis.asset_cols = asset_cols
            analysis.id_cols = id_cols

            analysis.total_all_asset_cols = sum(analysis.totals_by_col.get(c, Decimal("0")) for c in asset_cols)
            analysis.total_all_asset_cols_visible_only = sum(
                analysis.totals_by_col.get(f"vis:{c}", Decimal("0")) for c in asset_cols
            )
            return analysis
    except KeyError:
        return SheetAnalysis(file_label=file_label, sheet_name=sheet.name, sheet_path=sheet.path, notes=["Missing sheet xml"])


def analyze_workbook(
    path: Path,
    *,
    file_label: str,
    header_search_rows: int,
    asset_header_patterns: list[re.Pattern[str]],
) -> list[SheetAnalysis]:
    with zipfile.ZipFile(path, "r") as z:
        shared = _get_shared_strings(z)
        sheets = _get_sheets(z)
        analyses: list[SheetAnalysis] = []
        for sheet in sheets:
            analyses.append(
                analyze_worksheet(
                    z,
                    shared,
                    sheet,
                    file_label=file_label,
                    header_search_rows=header_search_rows,
                    asset_header_patterns=asset_header_patterns,
                )
            )
        return analyses


def _format_ids(ids: dict[str, str]) -> str:
    if not ids:
        return ""
    parts = [f"{k}={v}" for k, v in sorted(ids.items(), key=lambda kv: kv[0])]
    return "; ".join(parts)


def _find_subset_match(
    rows: list[ParsedRow],
    target: Decimal,
    *,
    max_rows: int = 2000,
    max_subset_size: int = 4,
    tolerance: Decimal = Decimal("0.05"),
) -> list[ParsedRow]:
    # heuristic: try to find 1..N rows whose AssetSum matches Target (within tolerance)
    cand = [r for r in rows if r.asset_sum != 0]
    cand.sort(key=lambda r: abs((r.asset_sum - target)))
    cand = cand[:max_rows]

    def close(a: Decimal, b: Decimal) -> bool:
        return abs(a - b) <= tolerance

    if max_subset_size >= 1:
        for r in cand:
            if close(r.asset_sum, target):
                return [r]

    if max_subset_size >= 2:
        for i in range(len(cand)):
            for j in range(i + 1, len(cand)):
                s = cand[i].asset_sum + cand[j].asset_sum
                if close(s, target):
                    return [cand[i], cand[j]]

    if max_subset_size >= 3:
        for i in range(len(cand)):
            for j in range(i + 1, len(cand)):
                for k in range(j + 1, len(cand)):
                    s = cand[i].asset_sum + cand[j].asset_sum + cand[k].asset_sum
                    if close(s, target):
                        return [cand[i], cand[j], cand[k]]

    if max_subset_size >= 4:
        for i in range(len(cand)):
            for j in range(i + 1, len(cand)):
                for k in range(j + 1, len(cand)):
                    for m in range(k + 1, len(cand)):
                        s = cand[i].asset_sum + cand[j].asset_sum + cand[k].asset_sum + cand[m].asset_sum
                        if close(s, target):
                            return [cand[i], cand[j], cand[k], cand[m]]

    return []


def _pick_main_sheet(analyses: list[SheetAnalysis]) -> Optional[SheetAnalysis]:
    candidates = [a for a in analyses if a.header_row and a.asset_cols and a.rows]
    if not candidates:
        return None
    candidates.sort(key=lambda a: len(a.rows), reverse=True)
    return candidates[0]


def _fmt_money(d: Decimal) -> str:
    # keep 2 decimals for human matching; no thousands separators to keep copy/paste easy
    q = d.quantize(Decimal("0.01"))
    return f"{q:f}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Identify inventory valuation total mismatch culprit rows.")
    ap.add_argument("--zoho", required=True, help="Zoho Inventory Valuation Summary export (.xlsx)")
    ap.add_argument("--adjustment", help="Inventory adjustment / opening stock import file (.xlsx)")
    ap.add_argument("--zoho-displayed-total", type=str, help="Total shown by Zoho report (e.g. 3864377.32)")
    ap.add_argument("--manual-sum", type=str, help="Your manual sum from the exported file (e.g. 3863785.33)")
    ap.add_argument("--out", default="inventory_culprit_report.txt", help="Output report path (txt)")
    ap.add_argument("--header-search-rows", type=int, default=60, help="How many top rows to scan for header row")
    ap.add_argument("--subset-max-size", type=int, default=4, help="Try to match delta with 1..N rows")
    ap.add_argument("--tolerance", type=str, default="0.05", help="Subset match tolerance (e.g. 0.05)")

    args = ap.parse_args()

    zoho_path = Path(args.zoho)
    adj_path = Path(args.adjustment) if args.adjustment else None
    out_path = Path(args.out)

    zoho_displayed = _parse_decimal(args.zoho_displayed_total) if args.zoho_displayed_total else None
    manual_sum = _parse_decimal(args.manual_sum) if args.manual_sum else None
    tolerance = _parse_decimal(args.tolerance) or Decimal("0.05")

    zoho_patterns = [re.compile(r"\binventory\s+asset\s+value\b", re.IGNORECASE)]
    adj_patterns = [
        re.compile(r"\b(inventory\s+asset\s+value|amount|value|stock\s*value|asset\s*value)\b", re.IGNORECASE)
    ]

    zoho_analyses = analyze_workbook(
        zoho_path,
        file_label="ZOHO_REPORT",
        header_search_rows=args.header_search_rows,
        asset_header_patterns=zoho_patterns,
    )
    adj_analyses: list[SheetAnalysis] = []
    if adj_path:
        adj_analyses = analyze_workbook(
            adj_path,
            file_label="ADJUSTMENT",
            header_search_rows=args.header_search_rows,
            asset_header_patterns=adj_patterns,
        )

    lines: list[str] = []
    lines.append(f"Inventory culprit report run: {_dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Zoho report: {zoho_path}")
    if adj_path:
        lines.append(f"Adjustment file: {adj_path}")
    lines.append("")

    def append_sheet_summary(a: SheetAnalysis) -> None:
        lines.append(f"[{a.file_label}] Sheet: {a.sheet_name} ({a.sheet_path})")
        if not a.header_row:
            lines.append('  Header row: NOT FOUND (no matching "Inventory Asset Value" header detected)')
            lines.append("")
            return
        lines.append(f"  Header row: {a.header_row}")
        lines.append(f"  Asset cols: {','.join(str(c) for c in a.asset_cols)}")
        lines.append(f"  Total (all asset cols): {_fmt_money(a.total_all_asset_cols)}")
        lines.append(f"  Total (visible rows only): {_fmt_money(a.total_all_asset_cols_visible_only)}")
        for col in sorted(a.asset_cols):
            hdr = a.header_map.get(col, f"col{col}")
            s = a.totals_by_col.get(col, Decimal('0'))
            lines.append(f"    Col {col} [{hdr}] sum: {_fmt_money(s)}")
        hidden_sum = a.total_all_asset_cols - a.total_all_asset_cols_visible_only
        if hidden_sum != 0:
            lines.append(f"  Hidden-only sum (all - visible): {_fmt_money(hidden_sum)}")
        lines.append("")

    for a in zoho_analyses:
        append_sheet_summary(a)
    for a in adj_analyses:
        append_sheet_summary(a)

    zoho_main = _pick_main_sheet(zoho_analyses)
    if zoho_main and zoho_main.header_row:
        lines.append(f"Zoho main sheet chosen for culprit search: {zoho_main.sheet_name}")
        lines.append("")

    if zoho_displayed is not None:
        computed = sum((a.total_all_asset_cols for a in zoho_analyses if a.header_row), Decimal("0"))
        delta_displayed_vs_computed = zoho_displayed - computed
        lines.append(f"Zoho displayed total (given): {_fmt_money(zoho_displayed)}")
        lines.append(f"Computed total from xlsx (sum of detected asset cols): {_fmt_money(computed)}")
        lines.append(f"Delta (displayed - computed): {_fmt_money(delta_displayed_vs_computed)}")
        lines.append("")

        if zoho_main:
            # Try matching the delta against hidden rows first (common when user sums only visible rows)
            hidden_rows = [r for r in zoho_main.rows if r.hidden]
            all_rows = zoho_main.rows

            if hidden_rows:
                matches = _find_subset_match(
                    hidden_rows,
                    delta_displayed_vs_computed,
                    max_subset_size=args.subset_max_size,
                    tolerance=tolerance,
                )
                if matches:
                    lines.append("Potential culprit (hidden) row(s) (assetSum ~= delta):")
                    for r in matches:
                        lines.append(
                            f"  Row {r.row_num} hidden={r.hidden} assetSum={_fmt_money(r.asset_sum)} id={{{_format_ids(r.ids)}}}"
                        )
                    lines.append("")

            matches = _find_subset_match(
                all_rows,
                delta_displayed_vs_computed,
                max_subset_size=args.subset_max_size,
                tolerance=tolerance,
            )
            if matches:
                lines.append("Potential culprit row(s) (assetSum ~= delta):")
                for r in matches:
                    lines.append(
                        f"  Row {r.row_num} hidden={r.hidden} assetSum={_fmt_money(r.asset_sum)} id={{{_format_ids(r.ids)}}}"
                    )
                lines.append("")
            else:
                lines.append(f"No subset of 1..{args.subset_max_size} rows matched the delta (within tolerance).")
                lines.append("")

    if manual_sum is not None and zoho_main:
        # Often the manual sum was done on visible rows only (filtered Excel)
        delta_manual_vs_visible = manual_sum - zoho_main.total_all_asset_cols_visible_only
        delta_displayed_vs_manual = (zoho_displayed - manual_sum) if zoho_displayed is not None else None

        lines.append(f"Manual sum (given): {_fmt_money(manual_sum)}")
        lines.append(f"Manual - visibleTotal: {_fmt_money(delta_manual_vs_visible)}")
        if delta_displayed_vs_manual is not None:
            lines.append(f"Displayed - manual: {_fmt_money(delta_displayed_vs_manual)}")
        lines.append("")

        if delta_displayed_vs_manual is not None:
            matches = _find_subset_match(
                zoho_main.rows,
                delta_displayed_vs_manual,
                max_subset_size=args.subset_max_size,
                tolerance=tolerance,
            )
            if matches:
                lines.append("Potential culprit row(s) (assetSum ~= displayed-manual):")
                for r in matches:
                    lines.append(
                        f"  Row {r.row_num} hidden={r.hidden} assetSum={_fmt_money(r.asset_sum)} id={{{_format_ids(r.ids)}}}"
                    )
                lines.append("")

    if adj_path and adj_analyses and zoho_main:
        # Compare overall totals (best-effort)
        adj_total = sum((a.total_all_asset_cols for a in adj_analyses if a.header_row), Decimal("0"))
        zoho_total = sum((a.total_all_asset_cols for a in zoho_analyses if a.header_row), Decimal("0"))
        lines.append(f"Zoho total (detected): {_fmt_money(zoho_total)}")
        lines.append(f"Adjustment total (detected): {_fmt_money(adj_total)}")
        lines.append(f"Delta (zoho - adjustment): {_fmt_money(zoho_total - adj_total)}")
        lines.append("")

    # Notes
    notes = sorted(set([n for a in zoho_analyses + adj_analyses for n in a.notes if n]))
    if notes:
        lines.append("Notes:")
        for n in notes:
            lines.append(f"  - {n}")
        lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote report to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
