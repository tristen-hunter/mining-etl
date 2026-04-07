import os
from datetime import datetime

# Path Configuration
BASE_DIR = r"C:\Users\trist\dev\mining-etl"
MASTER_DIR = os.path.join(BASE_DIR, "master")
DB_DIR = os.path.join(BASE_DIR, "DB")

# Configuration for each file type
FILE_CONFIG = {
    "olf_bookings.csv": {
        "dest": "olf_summary.csv",
        "skip_lines": 1,         # Start processing after line 1? No, we skip to data.
        "data_start_row": 1,     # Data actually starts here (0-indexed)
        "header_row_index": 0    # The specific row containing column names
    },
    "tons_report.csv": {
        "dest": "tons_reports.csv",
        "data_start_row": 1,
        "header_row_index": 0
    },
    "loading_list.csv": {
        "dest": "loading_list_summary.csv",
        "data_start_row": 1,
        "header_row_index": 0
    }
}

def get_date_string(year, month_name, day):
    try:
        date_obj = datetime.strptime(f"{year} {month_name} {day}", "%Y %B %d")
        return date_obj.strftime("%d.%m.%Y")
    except ValueError:
        return f"{day.zfill(2)}.{month_name}.{year}"

def is_empty_csv_line(line):
    """Returns True if the line is just commas and whitespace."""
    # Removes commas and whitespace; if nothing is left, the row is 'empty'
    return not line.replace(',', '').strip()

def process_files():
    if not os.path.exists(DB_DIR):
        os.makedirs(DB_DIR)

    for year in os.listdir(MASTER_DIR):
        year_path = os.path.join(MASTER_DIR, year)
        if not os.path.isdir(year_path): continue

        for month in os.listdir(year_path):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path): continue

            for day in os.listdir(month_path):
                day_path = os.path.join(month_path, day)
                if not os.path.isdir(day_path): continue

                date_str = get_date_string(year, month, day)
                
                for src_name, config in FILE_CONFIG.items():
                    src_file_path = os.path.join(day_path, src_name)
                    dest_file_path = os.path.join(DB_DIR, config["dest"])

                    if os.path.exists(src_file_path):
                        append_clean_data(src_file_path, dest_file_path, date_str, config)

def append_clean_data(src_path, dest_path, date_str, config):
    """Appends data with strict header mapping and comma-row filtering."""
    try:
        with open(src_path, 'r', encoding='utf-8') as src_file:
            lines = src_file.readlines()
            
            if len(lines) <= config["header_row_index"]:
                return 

            # Explicitly grab the header from the configured row
            header_line = lines[config["header_row_index"]].strip()
            # Start data from the configured row
            data_lines = lines[config["data_start_row"]:]

            file_exists = os.path.exists(dest_path)

            with open(dest_path, 'a', encoding='utf-8') as db_file:
                if not file_exists:
                    db_file.write(f"Date,{header_line}\n")
                
                for line in data_lines:
                    clean_line = line.strip()
                    # CRITICAL: Skip line if it's actually empty or just a string of commas
                    if clean_line and not is_empty_csv_line(clean_line):
                        db_file.write(f"{date_str},{clean_line}\n")
        
        print(f"  [Processed] {os.path.basename(src_path)} for {date_str}")
    except Exception as e:
        print(f"  [Error] {src_path}: {e}")

if __name__ == "__main__":
    print("Starting ETL: Cleaning empty rows and fixing headers...")
    process_files()
    print("Done.")