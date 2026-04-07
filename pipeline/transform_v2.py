"""
Pipeline:
    1. Scan archive year/month/day folders
    2. Bucket files into categories
    3. Validate day has all 3 categories (gate)
    4. Resolve each category to its master version
    5. Write to master/ with manifest
"""

import os
import json
import re
import pandas as pd
from datetime import datetime

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET_DIR = os.path.join(BASE_DIR, "archive", "2026")
MASTER_DIR = os.path.join(BASE_DIR, "master")
os.makedirs(MASTER_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def load_with_dynamic_header(path: str, anchor_col: str) -> pd.DataFrame | None:
    """
    Load an xls/xlsx/csv, auto-detect the header row by scanning for anchor_col,
    and return a clean DataFrame. Returns None if anchor not found.
    """
    try:
        if path.endswith(".csv"):
            raw = pd.read_csv(path, header=None).fillna("")
        elif path.endswith(".xls"):
            raw = pd.read_excel(path, header=None, engine="xlrd").fillna("")
        else:
            raw = pd.read_excel(path, header=None).fillna("")
    except Exception as e:
        print(f"    [LOAD ERROR] {os.path.basename(path)}: {e}")
        return None

    anchor_clean = anchor_col.lower().strip()
    header_idx = -1
    for i, row in raw.iterrows():
        if any(anchor_clean in str(v).lower().strip() for v in row):
            header_idx = i
            break

    if header_idx == -1:
        return None

    try:
        if path.endswith(".csv"):
            df = pd.read_csv(path, skiprows=header_idx)
        elif path.endswith(".xls"):
            df = pd.read_excel(path, skiprows=header_idx, engine="xlrd")
        else:
            df = pd.read_excel(path, skiprows=header_idx)
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        print(f"    [PARSE ERROR] {os.path.basename(path)}: {e}")
        return None


def drop_total_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove the footer/total row from tons reports.
    The total row has a blank/NaN 'Number' field AND a blank/NaN 'Registration' field.
    Every real data row has both populated (SCA... and a truck reg).
    """
    if "Number" in df.columns and "Registration" in df.columns:
        number_blank = df["Number"].isna() | df["Number"].astype(str).str.strip().eq("")
        reg_blank    = df["Registration"].isna() | df["Registration"].astype(str).str.strip().eq("")
        return df[~(number_blank & reg_blank)].reset_index(drop=True)
    # Fallback: drop rows where the first column is blank/NaN
    first_col = df.columns[0]
    not_blank = df[first_col].notna() & df[first_col].astype(str).str.strip().ne("")
    return df[not_blank].reset_index(drop=True)


def extract_version_number(filename: str) -> int:
    """
    Pull the version integer from a filename.
    Priority:
      1. Number inside parentheses: '(7)', '(1)' → 7, 1
      2. Number after a dash suffix before extension: 'name-4.xlsx' → 4
      3. Fall back to 0 (no version = base file)
    Deliberately ignores date digits (day/month/year) which are longer
    or appear as part of the date pattern DD.MM.YYYY / DD_MM_YYYY.
    """
    # 1. Parenthesised version — most explicit signal
    paren = re.findall(r'\((\d+)\)', filename)
    if paren:
        return max(int(n) for n in paren)
    # 2. Trailing dash-number before extension e.g. 'HMS - OLF 27.03.2026 (2)-4.xlsx' → 4
    #    Only match 1-2 digit numbers to avoid date components
    dash = re.findall(r'-(\d{1,2})\.(?:xlsx?|csv)$', filename, re.IGNORECASE)
    if dash:
        return max(int(n) for n in dash)
    # 3. No version marker found — this is the base/original file
    return 0


# ---------------------------------------------------------------------------
# CATEGORY RESOLVERS
# ---------------------------------------------------------------------------

def resolve_loading_list(paths: list[str]) -> tuple[pd.DataFrame | None, str]:
    """
    Rules:
      - Excel only (already filtered upstream)
      - Multiple versions possible: 'AMEND' in name, or version number like (1), (2)
      - Prefer highest version number; AMEND treated as version 999 (always wins)
      - mtime as tiebreaker when version numbers are equal
      - Load with 'Driver ID' as anchor (matches 'DRIVER ID.' with trailing period)
      - Return (df, source_path)
    """
    if not paths:
        return None, ""

    def sort_key(p):
        name = os.path.basename(p).upper()
        # AMEND always wins — treat as very high version
        if "AMEND" in name:
            version = 999
        else:
            version = extract_version_number(os.path.basename(p))
        return (version, os.path.getmtime(p))

    paths_sorted = sorted(paths, key=sort_key, reverse=True)
    chosen = paths_sorted[0]

    if len(paths) > 1:
        print(f"    [INFO] {len(paths)} loading lists found — selected: {os.path.basename(chosen)}")

    df = load_with_dynamic_header(chosen, "Driver ID")
    if df is None:
        print(f"    [FAIL] Could not parse loading list: {os.path.basename(chosen)}")
    return df, chosen


def resolve_olf(paths: list[str]) -> tuple[pd.DataFrame | None, str, list[str]]:
    """
    Rules:
      - Multiple versions of same doc throughout the day — want the most complete one
      - mtime is NOT reliable (WhatsApp exports all land with same timestamp)
      - Version numbers in filenames are NOT reliable (sender inconsistent)
      - Primary signal: row count — more valid truck rows = later/more complete version
      - Tiebreaker: highest version number in filename
      - Edge case: multiple contractor docs (different base names) — keep best of each
      - Load with 'ID' as anchor (catches 'ID/Passport')
      - Returns (df, winning_path, list_of_warnings)
    """
    if not paths:
        return None, "", []

    warnings = []

    def base_name(path):
        name = os.path.basename(path).lower()
        name = re.sub(r'\.xlsx?$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\(\d+\)', '', name)
        name = re.sub(r'(?<![a-z])-\d+$', '', name)
        name = re.sub(r'\d{2}[._\-]\d{2}[._\-]\d{4}', '', name)
        name = re.sub(r'[^a-z]+', '_', name).strip('_')
        return name

    groups: dict[str, list[str]] = {}
    for p in paths:
        key = base_name(p)
        groups.setdefault(key, []).append(p)

    if len(groups) > 1:
        print(f"    [INFO] Multiple OLF contractor docs detected: {list(groups.keys())}")

    def count_data_rows(path: str) -> int:
        df = load_with_dynamic_header(path, "ID")
        if df is None:
            return -1
        id_col = next((c for c in df.columns if 'id' in c.lower()), None)
        if id_col is None:
            return -1
        return int(df[id_col].notna().sum())

    best_per_group: list[str] = []
    for group_name, group_paths in groups.items():
        scored = []
        for p in group_paths:
            rows = count_data_rows(p)
            version = extract_version_number(os.path.basename(p))
            scored.append((rows, version, p))

        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        winner_rows, winner_ver, winner_path = scored[0]

        ties = [s for s in scored if s[0] == winner_rows and s[1] == winner_ver and s[2] != winner_path]
        if ties:
            tie_names = [os.path.basename(t[2]) for t in ties]
            msg = (f"OLF tie in group '{group_name}': {os.path.basename(winner_path)} "
                   f"and {tie_names} have identical scores — picked first alphabetically")
            warnings.append(msg)
            print(f"    [WARN] {msg}")

        best_per_group.append(winner_path)
        print(f"    [OLF] group '{group_name}': {len(group_paths)} versions → "
              f"selected {os.path.basename(winner_path)} ({winner_rows} trucks)")

    primary = sorted(best_per_group)[0]
    df = load_with_dynamic_header(primary, "ID")
    if df is None:
        print(f"    [FAIL] Could not parse OLF: {os.path.basename(primary)}")

    return df, primary, warnings


def resolve_tons_reports(paths: list[str]) -> tuple[pd.DataFrame | None, list[str]]:
    """
    Rules:
      - 1-3 reports per day, one per dig site
      - Concatenate all of them
      - Drop total/summary rows from each before concat
      - Anchor: 'Number' (the SCA transaction ID — always present on real rows)
      - Return (master_df, list_of_source_paths_used)
    """
    if not paths:
        return None, []

    dfs = []
    used = []

    for path in paths:
        df = load_with_dynamic_header(path, "Number")

        if df is None:
            print(f"    [SKIP] Could not parse tons report: {os.path.basename(path)}")
            continue

        df = drop_total_rows(df)

        # Tag which site this row came from (useful downstream)
        site_name = os.path.basename(path).upper()
        for keyword in ["HMS", "WESCOAL", "FUJAX", "KALAMIN", "LONDANI"]:
            if keyword in site_name:
                df["_source_site"] = keyword
                break
        else:
            df["_source_site"] = "UNKNOWN"

        dfs.append(df)
        used.append(path)

    if not dfs:
        return None, []

    master = pd.concat(dfs, ignore_index=True)
    return master, used


# ---------------------------------------------------------------------------
# BUCKETING
# ---------------------------------------------------------------------------

def bucket_files(day_path: str) -> dict[str, list[str]]:
    """
    Classify every spreadsheet in a day folder into one of three buckets.
    PDFs and anything unrecognised are silently skipped.
    """
    candidates: dict[str, list[str]] = {
        "loading_list": [],
        "tons_report":  [],
        "olf":          [],
    }

    for file in os.listdir(day_path):
        if not file.endswith((".xlsx", ".xls", ".csv")):
            continue

        path = os.path.join(day_path, file)
        name = file.lower()

        if "loading" in name:
            candidates["loading_list"].append(path)
        elif "tons" in name or "daily tons" in name:
            candidates["tons_report"].append(path)
        elif "olf" in name:
            candidates["olf"].append(path)
        # else: unrecognised — skip silently

    return candidates


# ---------------------------------------------------------------------------
# MANIFEST
# ---------------------------------------------------------------------------

def write_manifest(output_dir: str, date_str: str, manifest: dict) -> None:
    manifest["processed_at"] = datetime.now().isoformat(timespec="seconds")
    manifest["date"] = date_str
    manifest_path = os.path.join(output_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)


# ---------------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------------

def run_etl():
    skipped_days  = []
    complete_days = []

    for month in sorted(os.listdir(TARGET_DIR)):
        month_path = os.path.join(TARGET_DIR, month)
        if not os.path.isdir(month_path):
            continue

        for day in sorted(os.listdir(month_path)):
            day_path = os.path.join(month_path, day)
            if not os.path.isdir(day_path):
                continue

            date_str = f"2026/{month}/{day}"
            print(f"\n{'='*60}")
            print(f"Processing: {date_str}")

            # --- STAGE 1: BUCKET ---
            candidates = bucket_files(day_path)

            # --- STAGE 2: VALIDATE (gate) ---
            missing = [k for k, v in candidates.items() if not v]
            if missing:
                print(f"  [SKIP] Incomplete day — missing: {missing}")
                skipped_days.append(date_str)
                continue

            print(f"  [OK] All 3 categories present")
            print(f"       loading_list : {len(candidates['loading_list'])} file(s)")
            print(f"       tons_report  : {len(candidates['tons_report'])} file(s)")
            print(f"       olf          : {len(candidates['olf'])} file(s)")

            # --- STAGE 3: RESOLVE ---
            try:
                print(f"  [RESOLVING] loading_list...")
                loading_df, loading_src = resolve_loading_list(candidates["loading_list"])
                print(f"  [RESOLVING] olf...")
                olf_df, olf_src, olf_warnings = resolve_olf(candidates["olf"])
                print(f"  [RESOLVING] tons_report...")
                tons_df, tons_srcs = resolve_tons_reports(candidates["tons_report"])
            except Exception as e:
                import traceback
                print(f"  [ERROR] Unhandled exception during resolve: {e}")
                traceback.print_exc()
                skipped_days.append(date_str)
                continue

            # Secondary validation: did the files actually parse?
            parse_failures = []
            if loading_df is None: parse_failures.append("loading_list")
            if olf_df     is None: parse_failures.append("olf")
            if tons_df    is None: parse_failures.append("tons_report")

            if parse_failures:
                print(f"  [SKIP] Parse failures — could not read: {parse_failures}")
                skipped_days.append(date_str)
                continue

            # --- STAGE 4: WRITE TO MASTER ---
            try:
                output_dir = os.path.join(MASTER_DIR, "2026", month, day)
                os.makedirs(output_dir, exist_ok=True)

                loading_df.to_csv(os.path.join(output_dir, "loading_list.csv"),  index=False)
                olf_df.to_csv(    os.path.join(output_dir, "olf_bookings.csv"),   index=False)
                tons_df.to_csv(   os.path.join(output_dir, "tons_report.csv"),    index=False)
            except Exception as e:
                import traceback
                print(f"  [ERROR] Failed to write master files: {e}")
                traceback.print_exc()
                skipped_days.append(date_str)
                continue

            # --- STAGE 5: MANIFEST ---
            manifest = {
                "loading_list_source": os.path.basename(loading_src),
                "olf_source":          os.path.basename(olf_src),
                "olf_version_logic":   "highest row count, then highest version number",
                "olf_warnings":        olf_warnings,
                "tons_sources":        [os.path.basename(p) for p in tons_srcs],
                "tons_row_count":      len(tons_df),
            }
            write_manifest(output_dir, date_str, manifest)

            complete_days.append(date_str)
            print(f"  [DONE] Written to master/{month}/{day}")
            print(f"         loading : {os.path.basename(loading_src)}")
            print(f"         olf     : {os.path.basename(olf_src)}")
            print(f"         tons    : {[os.path.basename(p) for p in tons_srcs]}")

    # --- SUMMARY ---
    print(f"\n{'='*60}")
    print(f"COMPLETE: {len(complete_days)} days processed")
    print(f"SKIPPED : {len(skipped_days)} days")
    if skipped_days:
        print("Skipped days:")
        for d in skipped_days:
            print(f"  - {d}")


if __name__ == "__main__":
    run_etl()