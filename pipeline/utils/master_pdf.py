import json
from datetime import datetime
from fpdf import FPDF

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS & CONFIG
# ══════════════════════════════════════════════════════════════════════════════
MARGIN = 15
SAFE_BOTTOM = 25
HEADER_H = 8
ROW_H = 7

# ══════════════════════════════════════════════════════════════════════════════
#  PDF CLASS
# ══════════════════════════════════════════════════════════════════════════════
class ForensicPDF(FPDF):
    """Custom FPDF subclass for Mining Reports."""

    def __init__(self, report_date: str = ""):
        super().__init__()
        self.report_date = report_date
        self.set_margins(MARGIN, 15, MARGIN)
        self.set_auto_page_break(auto=True, margin=SAFE_BOTTOM)

    def header(self):
        self.set_font("Arial", "B", 11)
        self.cell(0, 9, "MINING OPERATIONS - SUMMARY REPORT", ln=False, align="C")
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

    def section_title(self, title: str):
        self.ln(4)
        self.set_fill_color(30, 50, 80)
        self.set_text_color(255, 255, 255)
        self.set_font("Arial", "B", 10)
        self.cell(0, 9, f"  {title}", ln=True, fill=True)
        self.set_text_color(0, 0, 0)
        self.set_font("Arial", "", 9)
        self.ln(2)

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

    def table_header(self, cols: list, widths: list):
        self.set_fill_color(30, 50, 80)
        self.set_text_color(255, 255, 255)
        self.set_font("Arial", "B", 8)
        for col, w in zip(cols, widths):
            self.cell(w, HEADER_H, col, border=1, fill=True, align="C")
        self.ln()
        self.set_text_color(0, 0, 0)
        self.set_font("Arial", "", 8)

# ══════════════════════════════════════════════════════════════════════════════
#  REPORT GENERATION LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def create_pdf_report(data, output_filename):
    # Use current date or extract from metadata if available
    report_date_str = datetime.now().strftime("%Y-%m-%d")
    pdf = ForensicPDF(report_date=report_date_str)
    pdf.add_page()

    # Section 1: Executive Summary (Mapping from 'kpis')
    pdf.section_title("EXECUTIVE SUMMARY")
    kpis = data.get("kpis", {})
    metadata = data.get("report_metadata", {})

    pdf.kv_row("Reporting Period (Days)", str(metadata.get("period_days", "N/A")))
    pdf.kv_row("Total Trips Recorded", str(metadata.get("total_trips_recorded", "0")))
    pdf.kv_row("Total Tons Moved", f"{kpis.get('total_tons_moved', 0):,.2f} t")
    pdf.kv_row("Avg Coal Mined / Day", f"{kpis.get('avg_coal_mined_per_day', 0):,.2f} t")
    pdf.kv_row("Avg Cycle Time", f"{kpis.get('avg_truck_turnaround_mins', 0):,.2f} min")
    pdf.kv_row("Avg Nett Mass / Load", f"{kpis.get('avg_nett_mass_per_load', 0):,.2f} t")

    # Section 2: Transporter Breakdown
    pdf.section_title("LOADS BY TRANSPORTER")
    pdf.table_header(["Transporter", "Total Loads"], [140, 40])
    
    transporters = data.get("transporter_breakdown", {})
    # If it's a dict, iterate items; if it's already sorted list of tuples, handle accordingly
    for name, loads in transporters.items():
        if loads > 0:
            pdf.cell(140, ROW_H, str(name), border=1)
            pdf.cell(40, ROW_H, str(int(loads)), border=1, align="C")
            pdf.ln()

    # Section 3: Truck Lists
    pdf.add_page()
    truck_analysis = data.get("truck_analysis", {})
    
    pdf.section_title(f"ACTIVE TRUCKS LIST ({truck_analysis.get('active_trucks_count', 0)})")
    pdf.set_font("Arial", "", 8)
    col_width = 60
    
    active_list = truck_analysis.get("list_active_trucks", [])
    for i, truck in enumerate(active_list):
        pdf.cell(col_width, 6, f"- {truck}")
        if (i + 1) % 3 == 0: pdf.ln()
    
    pdf.ln(10)
    pdf.section_title(f"INACTIVE TRUCKS ({truck_analysis.get('inactive_trucks_count', 0)})")
    
    inactive_list = truck_analysis.get("list_inactive_trucks", [])
    if not inactive_list:
        pdf.cell(0, 10, "All registered trucks moved coal during this period.", ln=True)
    else:
        for i, truck in enumerate(inactive_list):
            pdf.cell(col_width, 6, f"- {truck}")
            if (i + 1) % 3 == 0: pdf.ln()

    pdf.output(output_filename)
    print(f"Report successfully saved as {output_filename}")

if __name__ == "__main__":
    # This only runs for testing
    test_data = { "kpis": {}, "report_metadata": {"period_days": 0}, "transporter_breakdown": {}, "truck_analysis": {} }
    create_pdf_report(test_data, "Test_Report.pdf")