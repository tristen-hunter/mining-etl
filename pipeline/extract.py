import os
import re
import shutil
from datetime import datetime

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.join(os.path.dirname(__file__), '..', 'mining-data')
DEST_DIR = os.path.join(os.path.dirname(__file__), '..', 'archive')

MASTER_DIR = os.path.join(os.path.dirname(__file__), '..', 'master')
os.makedirs(MASTER_DIR, exist_ok=True)
os.makedirs(DEST_DIR, exist_ok=True)

print(f"Source : {SOURCE_DIR}")
print(f"Dest   : {DEST_DIR}\n")

# ── Constants ──────────────────────────────────────────────────────────────────

MONTHS = {
    "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3,
    "APRIL": 4, "MAY": 5, "JUNE": 6, "JULY": 7,
    "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10,
    "NOVEMBER": 11, "DECEMBER": 12
}

DATE_PATTERNS = [
    # 1. YYYY-MM-DD (High Confidence)
    r"(?P<year>20\d{2})[.\-_/](?P<month>\d{1,2})[.\-_/](?P<day>\d{1,2})",
    # 2. DD-MM-YYYY (High Confidence)
    r"(?P<day>\d{1,2})[.\-_/ ](?P<month>\d{1,2})[.\-_/ ](?P<year>20\d{2})",
    # 3. DD MONTH YYYY
    r"(?P<day>\d{1,2})\s+(?P<month_name>[A-Z]{3,})\s+(?P<year>20\d{2})",
    # 4. DD.MM (Short - use current year/month)
    r"(?P<day>\d{1,2})[.\-_/ ](?P<month>\d{1,2})(?!\d)",
    # 5. Just the Day (Very Low Confidence - match at end of string)
    r"(?P<day>\d{1,2})$"
]

DATE_STRIP_PATTERNS = [
    r"\d{1,2}[.\-_/]\d{1,2}[.\-_/]20\d{2}",   # DD.MM.YYYY / DD-MM-YYYY etc.
    r"20\d{2}[.\-_/]\d{1,2}[.\-_/]\d{1,2}",    # YYYY-MM-DD
    r"\d{1,2}\s+[A-Z]{3,}\s+20\d{2}",           # DD MONTH YYYY
    r"\d{1,2}[.\-_/ ]\d{1,2}(?!\d)",            # DD.MM (short)
]

# WhatsApp forwarding artifacts left behind after the date is stripped.
# These are sequence/order numbers the sender prepended to the filename.
# Order matters — longest/most-specific patterns must come first.
JUNK_STRIP_PATTERNS = [
    r'\s*-\s*\.\d+-\d+',      # ' - .2-3'
    r'\s*-\s*\.\d+',          # ' - .206', ' - .2'
    r'\s*-\s*\(\d+\)-\d+',    # ' - (2)-4'
    r'\s*-\s*\(\d+\)',         # ' - (2)'
    r'\s*-\s*-\d+',           # ' - -1', ' - -2'
    r'\s*\.\s*xlsx\s*',       # '.xlsx' embedded mid-name (amended file edge case)
    r'\.(?=\s|\(|$)',          # lone dot before space, '(', or end of string
    r'\s*-\s*$',               # trailing lone dash
]

CURRENT_YEAR = datetime.now().year

# ── Helpers ────────────────────────────────────────────────────────────────────

def extract_date(filename):
    name = filename.upper().replace('_', ' ')

    for i, pattern in enumerate(DATE_PATTERNS):
        match = re.search(pattern, name)
        if not match:
            continue

        parts = match.groupdict()
        raw_year = parts.get('year')

        if raw_year:
            if len(raw_year) == 4:
                year = int(raw_year)
            elif len(raw_year) == 2:
                year = 2000 + int(raw_year)
            else:
                # 1 or 3 digit year (e.g. '206') is a bad match — use current year
                year = CURRENT_YEAR
        else:
            year = CURRENT_YEAR

        if parts.get('month_name'):
            month = MONTHS.get(parts['month_name'])
            if month is None:
                continue
        elif parts.get('month'):
            month = int(parts['month'])
        else:
            month = datetime.now().month

        day = int(parts['day'])

        if parts.get('month') and day <= 12 and month <= 12 and i >= 1:
            print(f"   [AMBIGUOUS] '{match.group()}' in '{filename}'...")

        try:
            return datetime(year, month, day)
        except (ValueError, TypeError):
            continue

    return None


def rename_file(filename, date_obj):
    """
    1. Strip the leading 9-character WhatsApp prefix (e.g. '00003245-').
    2. Remove the raw date from wherever it sits in the name.
    3. Strip known WhatsApp junk fragments left behind after date removal.
    4. Append the date reformatted as DD.MM.YYYY at the end.
    5. Preserve the file extension.
    """
    name, ext = os.path.splitext(filename)

    # Step 1 — strip the leading 9-character prefix
    name = name[9:]

    # Step 2 — strip the date (longer/more-specific patterns first)
    cleaned = name
    for pattern in DATE_STRIP_PATTERNS:
        cleaned, n_subs = re.subn(pattern, '', cleaned, flags=re.IGNORECASE)
        if n_subs:
            break

    # Step 3 — strip WhatsApp junk fragments
    for pattern in JUNK_STRIP_PATTERNS:
        cleaned = re.sub(pattern, ' ', cleaned, flags=re.IGNORECASE)

    # Step 4 — normalise whitespace/punctuation
    cleaned = re.sub(r'[\s\-]+$', '', cleaned).strip()
    cleaned = re.sub(r'\s{2,}', ' ', cleaned)

    # Step 5 — append standardised date
    formatted_date = date_obj.strftime("%d.%m.%Y")
    new_name = f"{cleaned} {formatted_date}".strip()

    return new_name + ext


def safe_dest_path(folder, filename):
    dest = os.path.join(folder, filename)
    if not os.path.exists(dest):
        return dest
    base, ext = os.path.splitext(filename)
    counter = 1
    while os.path.exists(dest):
        dest = os.path.join(folder, f"{base} ({counter}){ext}")
        counter += 1
    return dest


# ── Process files ──────────────────────────────────────────────────────────────

stats = {"success": 0, "undated": 0}
IGNORE_EXTENSIONS = ['.txt', '.py', '.exe', '.ini']

for filename in sorted(os.listdir(SOURCE_DIR)):
    file_path = os.path.join(SOURCE_DIR, filename)

    name_part, ext = os.path.splitext(filename)

    if not os.path.isfile(file_path):
        continue
    if ext.lower() in IGNORE_EXTENSIONS or filename.startswith('_'):
        print(f"SKIPPING : {filename} (Ignored type/prefix)")
        continue

    date_obj = extract_date(filename)

    if not date_obj:
        undated_folder = os.path.join(DEST_DIR, str(CURRENT_YEAR), "_UNDATED")
        os.makedirs(undated_folder, exist_ok=True)
        dest = safe_dest_path(undated_folder, filename)
        shutil.copy2(file_path, dest)
        print(f"UNDATED  : {filename}")
        stats["undated"] += 1
        continue

    new_filename = rename_file(filename, date_obj)

    year_str      = str(date_obj.year)
    month_str     = date_obj.strftime("%B")
    day_str       = f"{date_obj.day:02d}"
    target_folder = os.path.join(DEST_DIR, year_str, month_str, day_str)
    os.makedirs(target_folder, exist_ok=True)

    dest = safe_dest_path(target_folder, new_filename)
    shutil.copy2(file_path, dest)
    print(f"SUCCESS  : {filename}")
    print(f"       →  {year_str}/{month_str}/{day_str}/{new_filename}")
    stats["success"] += 1

# ── Summary ────────────────────────────────────────────────────────────────────

print(f"""
─────────────────────────────
 Processing Complete
 Organised : {stats['success']}
 Undated   : {stats['undated']}
─────────────────────────────""")