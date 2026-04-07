"""
mining-etl/etl.py

Pipeline:
    1. Scan archive folders
    2. Bucket files (Loading, OLF, Tons)
    3. Resolve versions (Highest row count + versioning)
    4. Fuzzy Column Mapping (Fixes the "Empty CSV" bug)
    5. Write Master Files
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
# FUZZY MAPPING ENGINE
# ---------------------------------------------------------------------------

def fuzzy_map_columns(df_cols, standard_map):
    """
    Instead of exact matches, this finds the best column based on keywords.
    standard_map format: { "Standard Name": ["keyword1", "keyword2"] }
    """
    final_rename = {}
    clean_cols = [str(c).lower().strip() for c in df_cols]
    
    for std_name, keywords in standard_map.items():
        for i, col_name in enumerate(clean_cols):
            # If all keywords for a standard field are found in the column name
            if all(k in col_name for k in keywords):
                final_rename[df_cols[i]] = std_name
                break
    return final_rename

# Keyword definitions for OLF, Loading, and Tons
OLF_KEYWORDS = {
    "TRANSPORTER":   ["owner"],
    "HORSE REG":     ["horse", "reg"],
    "TRAILER 1 REG": ["trailer", "1"],
    "TRAILER 2 REG": ["trailer", "2"],
    "DRIVER NAME":   ["driver", "name"],
    "DRIVER ID":     ["id"]
}

LOADING_KEYWORDS = {
    "FLEET NUMBER":    ["fleet"],
    "MAKE OF VEHICLE": ["make"],
    "TRANSPORTER":     ["transporter"],
    "HORSE REG":       ["horse", "reg"],
    "TRAILER 1 REG":   ["trailer", "1"],
    "TRAILER 2 REG":   ["trailer", "2"],
    "DRIVER NAME":     ["driver", "name"],
    "DRIVER ID":       ["driver", "id"]
}

TONS_KEYWORDS = {
    "SCALE NUMBER":      ["number"],
    "CUSTOMER NAME":     ["customer"],
    "SUPPLIER NAME":     ["supplier"],
    "PRODUCT NAME":      ["product"],
    "HORSE REG":         ["registration"],
    "TRANSPORTER":       ["transporter"],
    "TRANSACTION TYPE":  ["type"],
    "FIRST DATE & TIME": ["first", "date"],
    "FIRST MASS":        ["first", "mass"],
    "SECOND DATE & TIME":["second", "date"],
    "SECOND MASS":       ["second", "mass"],
    "NET MASS":          ["nett"],
    "ORDER NUMBER":      ["order"],
    "DRIVER NAME":       ["driver", "name"],
    "DRIVER ID":         ["driver", "id"],
    "DESTINATION":       ["destination"]
}

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def load_with_dynamic_header(path: str, anchor_keywords: list) -> pd.DataFrame | None:
    try:
        ext = os.path.splitext(path)[1].lower()
        # Use engine='python' for better error handling on messy CSVs
        if ext == ".csv":
            raw = pd.read_csv(path, header=None, nrows=40).fillna("")
        else:
            raw = pd.read_excel(path, header=None, nrows=40).fillna("")

        header_idx = -1
        for i, row in raw.iterrows():
            row_str = " ".join([str(v).lower() for v in row])
            if all(k in row_str for k in anchor_keywords):
                header_idx = i
                break

        if header_idx == -1: return None

        if ext == ".csv":
            df = pd.read_csv(path, skiprows=header_idx)
        else:
            df = pd.read_excel(path, skiprows=header_idx)
            
        return df.loc[:, ~df.columns.str.contains('^Unnamed')].copy()
    except Exception as e:
        print(f"    [PARSE ERROR] {os.path.basename(path)}: {e}")
        return None

def drop_total_rows(df: pd.DataFrame) -> pd.DataFrame:
    # Logic: Real rows in Tons report start with SCA- in the first or second column
    for col in df.columns[:2]:
        mask = df[col].astype(str).str.contains('SCA', case=False, na=False)
        if mask.any():
            return df[mask].reset_index(drop=True)
    return df.dropna(how='all').reset_index(drop=True)

def extract_version_number(filename: str) -> int:
    paren = re.findall(r'\((\d+)\)', filename)
    if paren: return max(int(n) for n in paren)
    dash = re.findall(r'-(\d{1,2})\.(?:xlsx?|csv)$', filename, re.IGNORECASE)
    return max(int(n) for n in dash) if dash else 0

# ---------------------------------------------------------------------------
# RESOLVERS
# ---------------------------------------------------------------------------

def resolve_loading_list(paths: list[str]) -> tuple[pd.DataFrame | None, str]:
    if not paths: return None, ""
    def sort_key(p):
        v = 999 if "AMEND" in p.upper() else extract_version_number(p)
        return (v, os.path.getmtime(p))
    
    chosen = sorted(paths, key=sort_key, reverse=True)[0]
    df = load_with_dynamic_header(chosen, ["driver", "id"])
    if df is None: return None, chosen
    
    mapping = fuzzy_map_columns(df.columns, LOADING_KEYWORDS)
    df = df.rename(columns=mapping)
    
    cols = list(LOADING_KEYWORDS.keys())
    for c in cols: 
        if c not in df.columns: df[c] = pd.NA
    return df[cols], chosen

def resolve_olf(paths: list[str]) -> tuple[pd.DataFrame | None, list[str]]:
    if not paths: return None, []

    # Group by contractor/site name
    groups = {}
    for p in paths:
        name = re.sub(r'[^a-z]', '', os.path.basename(p).lower().split('olf')[0])
        groups.setdefault(name, []).append(p)

    final_dfs = []
    winning_paths = []

    for group_name, group_paths in groups.items():
        def score(p):
            df = load_with_dynamic_header(p, ["id", "passport"])
            if df is None: return (-1, 0, p)
            return (len(df.dropna(how='all')), extract_version_number(p), p)
        
        scored = sorted([score(p) for p in group_paths], reverse=True)
        winner_path = scored[0][2]
        winning_paths.append(winner_path)
        
        df = load_with_dynamic_header(winner_path, ["id", "passport"])
        if df is not None:
            mapping = fuzzy_map_columns(df.columns, OLF_KEYWORDS)
            df = df.rename(columns=mapping)
            cols = list(OLF_KEYWORDS.keys())
            for c in cols:
                if c not in df.columns: df[c] = pd.NA
            final_dfs.append(df[cols])

    if not final_dfs: return None, []
    return pd.concat(final_dfs, ignore_index=True), winning_paths

def resolve_tons(paths: list[str]) -> tuple[pd.DataFrame | None, list[str]]:
    if not paths: return None, []
    dfs, used = [], []
    for path in paths:
        df = load_with_dynamic_header(path, ["number", "mass"])
        if df is None: continue
        df = drop_total_rows(df)
        
        site = "UNKNOWN"
        for kw in ["HMS", "WESCOAL", "FUJAX", "KALAMIN", "LONDANI"]:
            if kw in path.upper(): site = kw; break
        df["SOURCE SITE"] = site
        
        mapping = fuzzy_map_columns(df.columns, TONS_KEYWORDS)
        df = df.rename(columns=mapping)
        dfs.append(df)
        used.append(path)
        
    if not dfs: return None, []
    master = pd.concat(dfs, ignore_index=True)
    cols = list(TONS_KEYWORDS.keys()) + ["SOURCE SITE"]
    for c in cols:
        if c not in master.columns: master[c] = pd.NA
    return master[cols], used

# ---------------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------------

def run_etl():
    for month in sorted(os.listdir(TARGET_DIR)):
        m_path = os.path.join(TARGET_DIR, month)
        if not os.path.isdir(m_path): continue

        for day in sorted(os.listdir(m_path)):
            d_path = os.path.join(m_path, day)
            if not os.path.isdir(d_path): continue

            print(f"Processing: 2026/{month}/{day}")
            files = [os.path.join(d_path, f) for f in os.listdir(d_path)]
            
            c = {
                "loading": [p for p in files if "loading" in p.lower()],
                "tons":    [p for p in files if "tons" in p.lower()],
                "olf":     [p for p in files if "olf" in p.lower()]
            }

            if not all(c.values()):
                print(f"  [SKIP] Missing categories")
                continue

            try:
                l_df, l_src = resolve_loading_list(c["loading"])
                o_df, o_srcs = resolve_olf(c["olf"])
                t_df, t_srcs = resolve_tons(c["tons"])

                if l_df is None or o_df is None or t_df is None: continue

                out = os.path.join(MASTER_DIR, "2026", month, day)
                os.makedirs(out, exist_ok=True)

                l_df.to_csv(os.path.join(out, "loading_list.csv"), index=False)
                o_df.to_csv(os.path.join(out, "olf_bookings.csv"), index=False)
                t_df.to_csv(os.path.join(out, "tons_report.csv"), index=False)
                
                with open(os.path.join(out, "manifest.json"), "w") as f:
                    json.dump({"loading": os.path.basename(l_src), "olf": [os.path.basename(p) for p in o_srcs], "tons": [os.path.basename(p) for p in t_srcs]}, f)
                print("  [DONE]")
            except Exception as e:
                print(f"  [ERROR] {e}")

if __name__ == "__main__":
    run_etl()