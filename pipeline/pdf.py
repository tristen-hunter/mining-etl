from fpdf import FPDF
from load import run_full_audit, load_data, BASE_PATH

class SimpleForensicPDF(FPDF):
    def header(self):
        self.set_font("Arial", "B", 12)
        self.cell(0, 10, "MINING OPERATIONS FRAUD DETECTION REPORT", ln=True, align="C")

    def add_section_title(self, title):
        self.ln(5)
        self.set_font("Arial", "B", 11)
        self.cell(0, 10, title, ln=True)
        self.set_font("Arial", "", 10)

def create_pdf_report(results, output_path):
    pdf = SimpleForensicPDF()
    pdf.add_page()
    
    s = results['summary']
    
    # Page 1: Key Findings [cite: 6, 7, 8, 10, 11]
    pdf.add_section_title("EXECUTIVE SUMMARY")
    pdf.cell(0, 8, f"Total Trips: {s['total_trips']}", ln=True)
    pdf.cell(0, 8, f"Total Net Mass: {s['total_net_mass']:,} kg", ln=True)
    pdf.cell(0, 8, f"Ghost Trucks Detected: {s['ghost_count']}", ln=True)
    pdf.cell(0, 8, f"Missing Authorized Trucks: {s['missing_count']} of {s['auth_count']}", ln=True)

    # Check 4: Scale Gaps [cite: 42, 43]
    pdf.add_section_title("CHECK 4: SCALE LOG GAPS")
    pdf.cell(0, 8, f"Range: SCA{s['sca_range'][0]} to SCA{s['sca_range'][1]}", ln=True)
    pdf.cell(0, 8, f"Status: {'CLEAR' if not results['gaps'] else f'FAIL - {len(results['gaps'])} gaps'}", ln=True)

    # Check 5 & 6: Operational [cite: 49, 53, 58]
    pdf.add_section_title("OPERATIONAL CHECKS")
    pdf.cell(0, 8, f"Fast Cycles (<10m): {len(results['dataframes']['fast_cycles'])} detected", ln=True)
    pdf.cell(0, 8, f"Tare Variance (>500kg): {len(results['dataframes']['high_tare'])} detected", ln=True)
    pdf.cell(0, 8, f"Under-loading (<32t): {len(results['dataframes']['underloaded'])} contractors", ln=True)

    # Appendix: Missing Fleet [cite: 60, 61]
    pdf.add_page()
    pdf.add_section_title("APPENDIX: FULL MISSING FLEET DETAIL")
    df_missing = results['dataframes']['no_shows']
    
    # Table Header
    pdf.set_font("Arial", "B", 9)
    pdf.cell(30, 8, "FLEET #", 1)
    pdf.cell(40, 8, "REG", 1)
    pdf.cell(50, 8, "TRANSPORTER", 1)
    pdf.cell(60, 8, "DRIVER", 1)
    pdf.ln()
    
    # Table Rows
    pdf.set_font("Arial", "", 8)
    for _, row in df_missing.iterrows():
        pdf.cell(30, 7, str(row['FLEET NUMBER']), 1)
        pdf.cell(40, 7, str(row['HORSE REG']), 1)
        pdf.cell(50, 7, str(row['TRANSPORTER']), 1)
        pdf.cell(60, 7, str(row['DRIVER NAME']), 1)
        pdf.ln()

    pdf.output(output_path)
    print(f"Report generated: {output_path}")

# --- RUN ---
if __name__ == "__main__":
    # Load the actual CSV data from the master/2026/March/19 path
    raw_data = load_data(BASE_PATH)
    
    if raw_data:
        # Run the forensic analysis to generate the results dictionary
        audit_results = run_full_audit(raw_data)
        
        if audit_results:
            # Generate the PDF using the dictionary
            create_pdf_report(audit_results, "Fraud_Report_Draft.pdf")
            print("Success: Fraud report generated.")
        else:
            print("Error: Audit failed to produce results.")
    else:
        print("Error: Could not load CSV files. Check your BASE_PATH.")