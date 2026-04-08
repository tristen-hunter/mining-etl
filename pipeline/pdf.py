"""
Mining Fraud PDF Report Generator  —  pdf.py
Renders the structured audit dict produced by load.py into a formatted PDF.
Uses fpdf2 (pip install fpdf2).
"""

import os
from fpdf import FPDF

# ── Layout constants ───────────────────────────────────────────────────────────
MARGIN      = 15
ROW_H       = 7
HEADER_H    = 8
SAFE_BOTTOM = 25


# ══════════════════════════════════════════════════════════════════════════════
#  PDF CLASS
# ══════════════════════════════════════════════════════════════════════════════

class ForensicPDF(FPDF):
    """Custom FPDF subclass with consistent header/footer and helper methods."""

    def __init__(self, report_date: str = ""):
        super().__init__()
        self.report_date = report_date
        self.set_margins(MARGIN, 15, MARGIN)
        self.set_auto_page_break(auto=True, margin=SAFE_BOTTOM)

    # ── Page furniture ────────────────────────────────────────────────────────
    def header(self):
        self.set_font("Arial", "B", 11)
        self.cell(0, 9, "MINING OPERATIONS - FRAUD DETECTION REPORT", ln=False, align="C")

        self.set_font("Arial", "", 8)
        self.cell(0, 9, self.report_date, ln=True, align="R")

        self.set_draw_color(180, 180, 180)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", "I", 8)
        self.set_text_color(130, 130, 130)
        self.cell(0, 10, f"Page {self.page_no()}", align="C")
        self.set_text_color(0, 0, 0)

    # ── Section heading ───────────────────────────────────────────────────────
    def section_title(self, title: str):
        self.ln(4)
        self.set_fill_color(30, 50, 80)
        self.set_text_color(255, 255, 255)
        self.set_font("Arial", "B", 10)
        self.cell(0, 9, f"  {title}", ln=True, fill=True)

        self.set_text_color(0, 0, 0)
        self.set_font("Arial", "", 9)
        self.ln(2)

    # ── Key-value row ─────────────────────────────────────────────────────────
    def kv_row(self, label: str, value: str, flag: bool = False):
        self.set_x(self.l_margin)

        fill = False
        if flag:
            self.set_fill_color(255, 230, 150)
            fill = True

        self.set_font("Arial", "B", 9)
        self.cell(75, 7, label, ln=False, fill=fill)

        self.set_font("Arial", "", 9)
        self.cell(0, 7, str(value), ln=True, fill=fill)

    # ── Table helpers ─────────────────────────────────────────────────────────
    def table_header(self, cols: list, widths: list):
        self.set_x(self.l_margin)

        self.set_fill_color(30, 50, 80)
        self.set_text_color(255, 255, 255)
        self.set_font("Arial", "B", 8)

        for col, w in zip(cols, widths):
            self.cell(w, HEADER_H, col, border=1, fill=True)

        self.ln()

        self.set_text_color(0, 0, 0)
        self.set_font("Arial", "", 8)

    def table_row(self, values: list, widths: list, shade: bool = False, flag: bool = False):
        self.set_x(self.l_margin)

        if flag:
            self.set_fill_color(255, 210, 210)
        elif shade:
            self.set_fill_color(240, 245, 255)
        else:
            self.set_fill_color(255, 255, 255)

        fill = flag or shade

        for val, w in zip(values, widths):
            self.cell(w, ROW_H, str(val)[:40], border=1, fill=fill)

        self.ln()

    def _paginated_table(self, cols, widths, rows, flag_col_idx=-1, flag_values=None):
        flag_values = flag_values or set()

        self.table_header(cols, widths)

        for i, row in enumerate(rows):

            if self.get_y() > (self.h - SAFE_BOTTOM - ROW_H):
                self.add_page()
                self.table_header(cols, widths)

            flagged = (
                flag_col_idx >= 0
                and str(row[flag_col_idx]) in flag_values
            )

            self.table_row(
                [str(v) if v is not None else "" for v in row],
                widths,
                shade=(i % 2 == 1),
                flag=flagged
            )


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION RENDERERS
# ══════════════════════════════════════════════════════════════════════════════

def _render_executive_summary(pdf, results):
    meta = results.get("meta", {})
    summary = results.get("summary", {})

    pdf.section_title("EXECUTIVE SUMMARY")

    pdf.kv_row("Report Date", meta.get("report_date", "N/A"))
    pdf.kv_row("Generated At", meta.get("generated_at", "N/A"))

    pdf.ln(2)

    pdf.kv_row("Total Trips", str(summary.get("total_trips", 0)))
    pdf.kv_row("Total Net Mass", f"{summary.get('total_net_tonnes', 0):.2f} t")
    pdf.kv_row("Unique Trucks", str(summary.get("unique_trucks_weighed", 0)))

    pdf.ln(2)

    pdf.kv_row("Ghost Trucks", str(summary.get("ghost_count", 0)),
               flag=summary.get("ghost_count", 0) > 0)

    pdf.kv_row("Missing Fleet", str(summary.get("missing_count", 0)),
               flag=summary.get("missing_count", 0) > 0)

    pdf.kv_row("Flagged Trucks", str(summary.get("flagged_truck_count", 0)),
               flag=summary.get("flagged_truck_count", 0) > 0)

    pdf.kv_row("SKA Gaps", str(summary.get("ska_gaps", 0)),
               flag=summary.get("ska_gaps", 0) > 0)

    pdf.kv_row(
        "Tampering Detected",
        "YES - rows may have been deleted" if summary.get("tampered") else "No evidence",
        flag=summary.get("tampered", False)
    )


def _render_ska_check(pdf: ForensicPDF, results: dict):
    ska = results["checks"].get("ska_sequence", {})
    pdf.section_title("SCALE SEQUENCE CHECK  (SKA Numbers)")

    pdf.kv_row("Range",             f"SCA{ska.get('min')}  ->  SCA{ska.get('max')}")
    pdf.kv_row("Expected entries",  str(ska.get("expected", 0)))
    pdf.kv_row("Present entries",   str(ska.get("present", 0)))
    pdf.kv_row("Missing entries",   str(len(ska.get("gaps", []))),
               flag=len(ska.get("gaps", [])) > 0)

    gaps = ska.get("gaps", [])
    if gaps:
        gap_str = ", ".join(f"SCA{g}" for g in gaps)
        pdf.set_font("Arial", "I", 8)
        pdf.set_text_color(180, 0, 0)
        pdf.multi_cell(0, 6, f"Missing: {gap_str}")
        pdf.set_text_color(0, 0, 0)
        pdf.ln(1)


def _render_tampering(pdf: ForensicPDF, results: dict):
    t = results["checks"].get("tampering", {})
    pdf.section_title("DATA TAMPERING CHECK")

    pdf.kv_row("Visible data rows",    str(t.get("row_count", "N/A")))
    pdf.kv_row("Computed sum (kg)",    f"{t.get('computed_sum_kg', 0):,.2f}")
    # pdf.kv_row("Grand Total row found", "Yes" if t.get("total_row_found") else "No")

    # if t.get("total_row_found"):
    #     pdf.kv_row("Reported total (kg)", f"{t.get('reported_total_kg', 0):,.2f}")
    #     disc = t.get("discrepancy_kg")
    #     pdf.kv_row(
    #         "Discrepancy (kg)",
    #         f"{disc:+,.2f}  <- ROWS DELETED" if disc and disc > 0 else f"{disc:+,.2f}  OK",
    #         flag=(disc is not None and disc > 0),
    #     )
    # else:
    #     pdf.set_font("Arial", "I", 9)
    #     pdf.set_text_color(130, 80, 0)
    #     pdf.cell(0, 7, "  No Grand Total row - request original weighbridge export.", ln=True)
    #     pdf.set_text_color(0, 0, 0)


def _render_operational_checks(pdf: ForensicPDF, results: dict):
    checks = results.get("checks", {})
    pdf.section_title("OPERATIONAL CHECKS SUMMARY")

    def _flag_line(label, key):
        count = checks.get(key, {}).get("count", 0)
        pdf.kv_row(label, str(count), flag=count > 0)
        if count > 0:
            regs = ", ".join(checks[key].get("trucks", []))
            pdf.set_font("Arial", "I", 8)
            pdf.set_text_color(130, 0, 0)
            pdf.multi_cell(0, 5, f"    Affected: {regs}")
            pdf.set_text_color(0, 0, 0)
            pdf.ln(1)

    _flag_line("Fast Loading Cycles",   "fast_cycle_flags")
    _flag_line("Tare Variance Flags",   "tare_flags")
    _flag_line("Fast Inter-Trip Gaps",  "fast_gap_flags")
    _flag_line("Under-Loaded Trucks",   "underload_flags")


def _render_ghost_trucks(pdf: ForensicPDF, results: dict):
    ghost_data = results["checks"].get("ghost_trucks", {}).get("records", [])
    if not ghost_data:
        return

    pdf.add_page()
    pdf.section_title(f"GHOST TRUCKS  ({len(ghost_data)} detected)")

    cols   = ["SCALE #", "REG", "TRANSPORTER", "DRIVER", "NET MASS (kg)"]
    widths = [30, 32, 55, 52, 31]

    rows = [
        [
            r.get("SCALE NUMBER", ""),
            r.get("HORSE REG", ""),
            r.get("TRANSPORTER", ""),
            r.get("DRIVER NAME", ""),
            f"{r.get('NET MASS', 0):,.0f}" if r.get("NET MASS") else "",
        ]
        for r in ghost_data
    ]
    pdf._paginated_table(cols, widths, rows)


def _render_missing_fleet(pdf: ForensicPDF, results: dict):
    missing = results["checks"].get("missing_fleet", {})
    total   = missing.get("count", 0)
    if total == 0:
        return

    pdf.add_page()
    pdf.section_title(f"MISSING AUTHORISED FLEET  ({total} trucks absent from weighbridge)")

    cols   = ["REG", "TRANSPORTER", "FLEET #", "DRIVER", "SOURCE"]
    widths = [30, 50, 22, 58, 40]

    all_rows = []
    for group in ("impangele", "other"):
        for r in missing.get(group, []):
            all_rows.append([
                r.get("HORSE REG", ""),
                r.get("TRANSPORTER", ""),
                r.get("FLEET NUMBER", "") or "",
                r.get("DRIVER NAME", ""),
                r.get("AUTH_SOURCE", ""),
            ])

    pdf._paginated_table(cols, widths, all_rows)


def _render_flagged_trucks(pdf: ForensicPDF, results: dict):
    trucks  = results.get("trucks", [])
    flagged = [t for t in trucks if t.get("flags")]
    if not flagged:
        return

    pdf.add_page()
    pdf.section_title(f"FLAGGED TRUCK ANALYSIS  ({len(flagged)} trucks)")

    cols   = ["REG", "TRIPS", "TOTAL (t)", "AVG (t)", "FLAGS"]
    widths = [30, 18, 28, 28, 96]

    rows = [
        [
            t["horse_reg"],
            str(t["production"]["trip_count"]),
            f"{t['production']['total_net_tonnes']:.2f}",
            f"{t['production']['avg_net_tonnes']:.2f}",
            " | ".join(t["flags"]),
        ]
        for t in flagged
    ]

    # Flag rows that are ghost trucks
    ghost_regs = {r[0] for r in rows if "GHOST_TRUCK" in r[4]}
    pdf._paginated_table(cols, widths, rows, flag_col_idx=0,
                         flag_values=ghost_regs)


def _render_transporter_summary(pdf: ForensicPDF, results: dict):
    transporters = results.get("transporters", [])
    if not transporters:
        return

    pdf.add_page()
    pdf.section_title("TRANSPORTER PRODUCTION SUMMARY")

    cols   = ["TRANSPORTER", "TRIPS", "TRUCKS", "TOTAL (t)", "AVG LOAD (t)", "UNDERLOAD?"]
    widths = [58, 18, 20, 30, 32, 42]

    rows = [
        [
            t.get("TRANSPORTER", ""),
            str(t.get("trips", 0)),
            str(t.get("unique_trucks", 0)),
            f"{t.get('total_net_tonnes', 0):.2f}",
            f"{t.get('avg_net_tonnes', 0):.2f}",
            "YES" if t.get("underload_flag") else "No",
        ]
        for t in transporters
    ]

    underload_transporters = {r[0] for r in rows if r[5] == "YES"}
    pdf._paginated_table(cols, widths, rows, flag_col_idx=0,
                         flag_values=underload_transporters)


# ══════════════════════════════════════════════════════════════════════════════
#  PUBLIC ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def create_pdf_report(results: dict, output_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    pdf = ForensicPDF(results.get("meta", {}).get("report_date", ""))
    pdf.add_page()

    _render_executive_summary(pdf, results)
    _render_ska_check(pdf, results)
    _render_tampering(pdf, results)
    _render_operational_checks(pdf, results)
    _render_flagged_trucks(pdf, results)
    # _render_ghost_trucks(pdf, results)
    _render_transporter_summary(pdf, results)
    _render_missing_fleet(pdf, results)

    pdf.output(output_path)
    print(f"  PDF saved -> {output_path}")