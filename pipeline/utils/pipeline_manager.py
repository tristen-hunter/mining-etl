# --- pipeline_manager.py ---
import os
from master_report import generate_mining_summary 
from master_pdf import create_pdf_report

def main():
    CSV_SOURCE = r"C:\Users\trist\dev\mining-etl\DB\reports_summary.csv"
    
    if not os.path.exists(CSV_SOURCE):
        print(f"Error: Source file '{CSV_SOURCE}' not found.")
        return

    # 1. Extract & Calculate (Returns the dictionary)
    data = generate_mining_summary(CSV_SOURCE)
    
    # 2. Generate PDF (Passes the dictionary directly to the PDF creator)
    days = data['report_metadata']['period_days']
    OUTPUT_FILE = f"Mining_Report_{days}_days.pdf"
    
    create_pdf_report(data, OUTPUT_FILE)
    
    print(f"Flow Complete. Report generated: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()