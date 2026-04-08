"""
Mining Fraud Audit Engine  —  load.py
Loads, cleans, and audits one day's CSV data.
Exposes process_day() and write_summary_csv() for pipeline use.
"""

import os
import pandas as pd
from typing import Optional

# ── Thresholds ─────────────────────────────────────────────────────────────────
TARE_VAR_LIMIT_KG = 750    # tare swing >= this is flagged
MIN_CYCLE_MINUTES = 10     # on-scale cycle below this is flagged
MIN_AVG_LOAD_TONS = 32.0   # average net mass below this (tonnes) is flagged

# ── CSV output path ────────────────────────────────────────────────────────────
DEFAULT_CSV_PATH = r"C:\Users\trist\dev\mining-etl\DB\reports_summary.csv"


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _norm_reg(series: pd.Series) -> pd.Series:
    """Uppercase, strip, collapse all internal whitespace."""
    return series.astype(str).str.strip().str.upper().str.replace(r"\s+", "", regex=True)


def _date_label_from_path(folder_path: str) -> str:
    """
    Derive a human-readable date label from the folder structure.
    Expected layout: …/master/YEAR/Month/Day
    Falls back to the raw folder name if the structure doesn't match.
    """
    parts = os.path.normpath(folder_path).split(os.sep)
    # Look for the last three path components that follow 'master'
    try:
        master_idx = [p.lower() for p in parts].index("master")
        year, month, day = parts[master_idx + 1], parts[master_idx + 2], parts[master_idx + 3]
        return f"{day} {month} {year}"
    except (ValueError, IndexError):
        # Graceful fallback: use the last folder component
        return os.path.basename(folder_path)


# ══════════════════════════════════════════════════════════════════════════════
#  1. DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

def _load_csv(path: str) -> Optional[pd.DataFrame]:
    """Load a single CSV, stripping column-name whitespace. Returns None on failure."""
    if not os.path.isfile(path):
        print(f"  [WARN] Not found: {path}")
        return None
    try:
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip()
        return df
    except Exception as exc:
        print(f"  [ERROR] Could not read {path}: {exc}")
        return None


def load_data(folder_path: str) -> dict:
    """
    Load the three source CSVs from folder_path.
    No fallback path — the pipeline must point to the correct folder.
    Parses and coerces numeric/datetime columns on the tons report.
    """
    file_map = {
        "tons":    "tons_report.csv",
        "loading": "loading_list.csv",
        "olf":     "olf_bookings.csv",
    }

    data = {}
    for key, filename in file_map.items():
        df = _load_csv(os.path.join(folder_path, filename))
        if df is not None and "HORSE REG" in df.columns:
            df["HORSE REG"] = _norm_reg(df["HORSE REG"])
        data[key] = df

    tons = data.get("tons")
    if tons is not None:
        for col in ("FIRST DATE & TIME", "SECOND DATE & TIME"):
            if col in tons.columns:
                tons[col] = pd.to_datetime(tons[col], dayfirst=True, errors="coerce")
        for col in ("NET MASS", "FIRST MASS", "SECOND MASS"):
            if col in tons.columns:
                tons[col] = pd.to_numeric(tons[col], errors="coerce")

    return data


# ══════════════════════════════════════════════════════════════════════════════
#  2. AUTHORISED FLEET
# ══════════════════════════════════════════════════════════════════════════════

def build_auth_master(df_load: pd.DataFrame, df_olf: pd.DataFrame) -> pd.DataFrame:
    """
    Merge Loading List + OLF into one de-duplicated authorised-fleet table.
    Loading List rows take priority (they carry more columns).
    """
    load_cols = ["HORSE REG", "FLEET NUMBER", "TRANSPORTER", "DRIVER NAME", "DRIVER ID"]
    olf_cols  = ["HORSE REG", "TRANSPORTER", "DRIVER NAME", "DRIVER ID"]

    load_clean = df_load.reindex(columns=load_cols).copy()
    load_clean["AUTH_SOURCE"] = "Loading List"

    olf_clean = df_olf.reindex(columns=olf_cols).copy()
    olf_clean["AUTH_SOURCE"] = "OLF Bookings"

    combined = pd.concat([load_clean, olf_clean], ignore_index=True)
    combined = combined.sort_values(
        "AUTH_SOURCE",
        key=lambda s: s.map({"Loading List": 0, "OLF Bookings": 1})
    ).drop_duplicates(subset=["HORSE REG"], keep="first").reset_index(drop=True)

    return combined


# ══════════════════════════════════════════════════════════════════════════════
#  3. GHOST & MISSING FLEET
# ══════════════════════════════════════════════════════════════════════════════

def find_ghost_trucks(df_tons: pd.DataFrame, auth_regs: set) -> pd.DataFrame:
    """Weighbridge records with no matching authorised registration."""
    cols = ["SCALE NUMBER", "HORSE REG", "TRANSPORTER", "DRIVER NAME",
            "FIRST DATE & TIME", "NET MASS"]
    ghosts = df_tons[~df_tons["HORSE REG"].isin(auth_regs)].copy()
    return ghosts.reindex(columns=cols).reset_index(drop=True)


def find_missing_fleet(auth_master: pd.DataFrame, tons_regs: set) -> pd.DataFrame:
    """Authorised trucks that never showed up on the scale."""
    missing = auth_master[~auth_master["HORSE REG"].isin(tons_regs)].copy()
    return missing.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
#  4. SKA SEQUENCE
# ══════════════════════════════════════════════════════════════════════════════

def analyse_ska_sequence(df_tons: pd.DataFrame) -> dict:
    """Detect gaps in the sequential SCA weighbridge numbers."""
    valid = df_tons["SCALE NUMBER"].astype(str).str.extract(r"SCA(\d+)", expand=False).dropna()
    nums  = valid.astype(int).sort_values()

    if nums.empty:
        return {"min": None, "max": None, "present": 0, "expected": 0, "gaps": []}

    full_range = set(range(nums.min(), nums.max() + 1))
    gaps       = sorted(full_range - set(nums))

    return {
        "min":      int(nums.min()),
        "max":      int(nums.max()),
        "present":  int(len(nums)),
        "expected": int(len(full_range)),
        "gaps":     gaps,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  5. TAMPERING CHECK
# ══════════════════════════════════════════════════════════════════════════════

def check_tampering(df_tons: pd.DataFrame) -> dict:
    """
    If a Grand Total row exists, compare the reported total to the computed
    row-sum.  A positive discrepancy means rows were deleted before delivery.
    """
    data_mask = df_tons["SCALE NUMBER"].astype(str).str.match(r"^SCA\d+$")
    data_rows = df_tons[data_mask]
    total_rows = df_tons[~data_mask]

    computed = float(data_rows["NET MASS"].sum())

    reported    = None
    discrepancy = None
    if not total_rows.empty:
        reported = pd.to_numeric(total_rows["NET MASS"].iloc[0], errors="coerce")
        if pd.notna(reported):
            discrepancy = round(float(reported) - computed, 2)

    return {
        "row_count":         int(len(data_rows)),
        "computed_sum_kg":   round(computed, 2),
        "total_row_found":   not total_rows.empty,
        "reported_total_kg": round(float(reported), 2) if reported is not None else None,
        "discrepancy_kg":    discrepancy,
        "tampered":          (discrepancy is not None and discrepancy > 0),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  6. PER-TRUCK ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

def _trip_interval_stats(trips: pd.DataFrame) -> dict:
    """
    Inter-trip gap = time between SECOND MASS of trip N and FIRST MASS of N+1.
    Represents the truck's turnaround / reload time.
    """
    if len(trips) < 2:
        return {
            "trip_count":     1,
            "gaps_minutes":   [],
            "avg_gap_min":    None,
            "min_gap_min":    None,
            "max_gap_min":    None,
            "fast_gap_count": 0,
        }

    trips = trips.sort_values("FIRST DATE & TIME").reset_index(drop=True)
    gaps  = []
    for i in range(1, len(trips)):
        prev_out = trips.loc[i - 1, "SECOND DATE & TIME"]
        this_in  = trips.loc[i,     "FIRST DATE & TIME"]
        if pd.notna(prev_out) and pd.notna(this_in):
            gaps.append(round((this_in - prev_out).total_seconds() / 60, 1))

    fast = [g for g in gaps if g < MIN_CYCLE_MINUTES]
    return {
        "trip_count":     len(trips),
        "gaps_minutes":   gaps,
        "avg_gap_min":    round(sum(gaps) / len(gaps), 1) if gaps else None,
        "min_gap_min":    min(gaps) if gaps else None,
        "max_gap_min":    max(gaps) if gaps else None,
        "fast_gap_count": len(fast),
    }


def _cycle_times(trips: pd.DataFrame) -> list:
    """Per-trip on-scale duration: FIRST MASS → SECOND MASS (minutes)."""
    cycles = []
    for _, row in trips.iterrows():
        t1, t2 = row.get("FIRST DATE & TIME"), row.get("SECOND DATE & TIME")
        if pd.notna(t1) and pd.notna(t2):
            cycles.append(round((t2 - t1).total_seconds() / 60, 1))
        else:
            cycles.append(None)
    return cycles


def _tare_analysis(trips: pd.DataFrame) -> dict:
    """Consistency of the empty-truck weight across all trips."""
    tares = trips["FIRST MASS"].dropna()
    if tares.empty:
        return {"min_kg": None, "max_kg": None, "avg_kg": None,
                "variance_kg": None, "flagged": False}
    variance = float(tares.max() - tares.min())
    return {
        "min_kg":      float(tares.min()),
        "max_kg":      float(tares.max()),
        "avg_kg":      round(float(tares.mean()), 0),
        "variance_kg": round(variance, 0),
        "flagged":     variance >= TARE_VAR_LIMIT_KG,
    }


def build_truck_profiles(df_tons: pd.DataFrame, auth_master: pd.DataFrame) -> list:
    """One rich profile dict per unique HORSE REG, with all fraud signals."""
    auth_lookup = auth_master.set_index("HORSE REG").to_dict(orient="index")
    profiles    = []

    for reg, trips in df_tons.groupby("HORSE REG"):
        trips         = trips.sort_values("FIRST DATE & TIME").reset_index(drop=True)
        net_masses    = trips["NET MASS"].dropna().tolist()
        cycle_times   = _cycle_times(trips)
        interval_info = _trip_interval_stats(trips)
        tare_info     = _tare_analysis(trips)

        total_net_kg = sum(net_masses)
        avg_net_kg   = total_net_kg / len(net_masses) if net_masses else 0

        trip_detail = []
        for i, (_, row) in enumerate(trips.iterrows()):
            cycle = cycle_times[i]
            trip_detail.append({
                "ska":           row.get("SCALE NUMBER"),
                "first_time":    str(row["FIRST DATE & TIME"])  if pd.notna(row.get("FIRST DATE & TIME"))  else None,
                "second_time":   str(row["SECOND DATE & TIME"]) if pd.notna(row.get("SECOND DATE & TIME")) else None,
                "first_mass_kg": row.get("FIRST MASS"),
                "second_mass_kg":row.get("SECOND MASS"),
                "net_mass_kg":   row.get("NET MASS"),
                "cycle_min":     cycle,
                "fast_cycle":    (cycle is not None and cycle < MIN_CYCLE_MINUTES),
                "driver":        row.get("DRIVER NAME"),
                "destination":   row.get("DESTINATION"),
                "transporter":   row.get("TRANSPORTER"),
            })

        auth_info = auth_lookup.get(reg, {})
        is_ghost  = reg not in auth_lookup

        flags = []
        if is_ghost:
            flags.append("GHOST_TRUCK")
        if tare_info["flagged"]:
            flags.append("TARE_VARIANCE")
        if interval_info["fast_gap_count"] > 0:
            flags.append("FAST_INTER_TRIP_GAP")
        if any(c is not None and c < MIN_CYCLE_MINUTES for c in cycle_times):
            flags.append("FAST_LOADING_CYCLE")
        if not is_ghost and avg_net_kg / 1000 < MIN_AVG_LOAD_TONS:
            flags.append("UNDER_LOADED")

        profiles.append({
            "horse_reg":         reg,
            "is_ghost":          is_ghost,
            "flags":             flags,
            "auth_source":       auth_info.get("AUTH_SOURCE"),
            "fleet_number":      auth_info.get("FLEET NUMBER"),
            "authorised_driver": auth_info.get("DRIVER NAME"),
            "transporter":       trips["TRANSPORTER"].mode().iloc[0] if not trips.empty else None,
            "production": {
                "trip_count":       len(trips),
                "total_net_kg":     round(total_net_kg, 2),
                "total_net_tonnes": round(total_net_kg / 1000, 2),
                "avg_net_kg":       round(avg_net_kg, 2),
                "avg_net_tonnes":   round(avg_net_kg / 1000, 2),
                "underloaded_avg":  avg_net_kg / 1000 < MIN_AVG_LOAD_TONS,
            },
            "tare":           tare_info,
            "trip_intervals": interval_info,
            "trips":          trip_detail,
        })

    profiles.sort(key=lambda p: (not p["is_ghost"], -len(p["flags"]), p["horse_reg"]))
    return profiles


# ══════════════════════════════════════════════════════════════════════════════
#  7. TRANSPORTER SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def build_transporter_summary(df_tons: pd.DataFrame) -> list:
    grp = df_tons.groupby("TRANSPORTER").agg(
        trips        =("NET MASS", "count"),
        total_net_kg =("NET MASS", "sum"),
        avg_net_kg   =("NET MASS", "mean"),
        unique_trucks=("HORSE REG", "nunique"),
    ).reset_index()
    grp["avg_net_tonnes"]   = (grp["avg_net_kg"]   / 1000).round(2)
    grp["total_net_tonnes"] = (grp["total_net_kg"] / 1000).round(2)
    grp["underload_flag"]   = grp["avg_net_tonnes"] < MIN_AVG_LOAD_TONS
    return grp.sort_values("total_net_kg", ascending=False).to_dict(orient="records")


# ══════════════════════════════════════════════════════════════════════════════
#  8. MASTER AUDIT FUNCTION
# ══════════════════════════════════════════════════════════════════════════════

def run_audit(data: dict, folder_path: str = "") -> dict:
    """
    Orchestrate all checks and return a single structured result dict.
    folder_path is used only to derive the human-readable report date label.
    Raises ValueError when mandatory source data is absent.
    """
    df_tons = data.get("tons")
    df_load = data.get("loading")
    df_olf  = data.get("olf")

    # ── Guard: mandatory inputs ────────────────────────────────────────────
    if df_tons is None:
        raise ValueError("tons_report.csv is required but could not be loaded.")
    if df_load is None and df_olf is None:
        raise ValueError(
            "At least one of loading_list.csv or olf_bookings.csv is required."
        )

    # ── Replace missing optional frames with empty stubs ──────────────────
    if df_load is None:
        print("  [WARN] loading_list.csv missing — proceeding without it.")
        df_load = pd.DataFrame(columns=["HORSE REG", "FLEET NUMBER",
                                        "TRANSPORTER", "DRIVER NAME", "DRIVER ID"])
    if df_olf is None:
        print("  [WARN] olf_bookings.csv missing — proceeding without it.")
        df_olf = pd.DataFrame(columns=["HORSE REG", "TRANSPORTER",
                                       "DRIVER NAME", "DRIVER ID"])

    # ── Core computations ─────────────────────────────────────────────────
    auth_master  = build_auth_master(df_load, df_olf)
    auth_regs    = set(auth_master["HORSE REG"].unique())
    tons_regs    = set(df_tons["HORSE REG"].unique())

    ghosts       = find_ghost_trucks(df_tons, auth_regs)
    no_shows     = find_missing_fleet(auth_master, tons_regs)
    ska          = analyse_ska_sequence(df_tons)
    tamper       = check_tampering(df_tons)
    trucks       = build_truck_profiles(df_tons, auth_master)
    transporters = build_transporter_summary(df_tons)

    # ── Flag aggregation ──────────────────────────────────────────────────
    flagged_trucks = [t for t in trucks if t["flags"]]
    tare_flags     = [t for t in trucks if "TARE_VARIANCE"      in t["flags"]]
    cycle_flags    = [t for t in trucks if "FAST_LOADING_CYCLE" in t["flags"]]
    gap_flags      = [t for t in trucks if "FAST_INTER_TRIP_GAP"in t["flags"]]
    under_flags    = [t for t in trucks if "UNDER_LOADED"        in t["flags"]]

    no_shows_impangele = no_shows[
        no_shows["TRANSPORTER"].astype(str).str.upper().str.contains("IMPANGELE", na=False)
    ]
    no_shows_other = no_shows[
        ~no_shows["TRANSPORTER"].astype(str).str.upper().str.contains("IMPANGELE", na=False)
    ]

    # ── Date label derived from folder path, not global variables ─────────
    report_date = _date_label_from_path(folder_path) if folder_path else "Unknown Date"

    return {
        "meta": {
            "report_date":  report_date,
            "folder_path":  folder_path,
            "generated_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
            "thresholds": {
                "tare_variance_kg":   TARE_VAR_LIMIT_KG,
                "fast_cycle_min":     MIN_CYCLE_MINUTES,
                "underload_avg_tons": MIN_AVG_LOAD_TONS,
            },
        },
        "summary": {
            "total_trips":           int(len(df_tons)),
            "total_net_kg":          round(float(df_tons["NET MASS"].sum()), 2),
            "total_net_tonnes":      round(float(df_tons["NET MASS"].sum()) / 1000, 2),
            "unique_trucks_weighed": int(df_tons["HORSE REG"].nunique()),
            "auth_fleet_size":       int(len(auth_regs)),
            "ghost_count":           int(len(ghosts)),
            "missing_count":         int(len(no_shows)),
            "missing_impangele":     int(len(no_shows_impangele)),
            "missing_other":         int(len(no_shows_other)),
            "flagged_truck_count":   int(len(flagged_trucks)),
            "ska_gaps":              int(len(ska["gaps"])),
            "tampered":              tamper["tampered"],
        },
        "checks": {
            "ghost_trucks": {
                "count":   int(len(ghosts)),
                "records": ghosts.to_dict(orient="records"),
            },
            "missing_fleet": {
                "count":     int(len(no_shows)),
                "impangele": no_shows_impangele.to_dict(orient="records"),
                "other":     no_shows_other.to_dict(orient="records"),
            },
            "ska_sequence": ska,
            "tampering":    tamper,
            "tare_flags": {
                "count":  len(tare_flags),
                "trucks": [t["horse_reg"] for t in tare_flags],
            },
            "fast_cycle_flags": {
                "count":  len(cycle_flags),
                "trucks": [t["horse_reg"] for t in cycle_flags],
            },
            "fast_gap_flags": {
                "count":  len(gap_flags),
                "trucks": [t["horse_reg"] for t in gap_flags],
            },
            "underload_flags": {
                "count":  len(under_flags),
                "trucks": [t["horse_reg"] for t in under_flags],
            },
        },
        "transporters": transporters,
        "trucks":        trucks,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  9. PIPELINE ENTRY POINT  (imported by run_pipeline.py)
# ══════════════════════════════════════════════════════════════════════════════

def process_day(folder_path: str) -> dict:
    """
    Top-level entry point for the pipeline.
    Loads the three CSVs from folder_path, runs the full audit, returns the
    result dict.  Raises ValueError if mandatory files are missing.
    """
    data   = load_data(folder_path)
    result = run_audit(data, folder_path=folder_path)
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  10. CSV SUMMARY EXPORT
# ══════════════════════════════════════════════════════════════════════════════

def _build_summary_rows(result: dict) -> pd.DataFrame:
    """Flatten one audit result into a tabular DataFrame (one row per truck)."""
    report_date = result["meta"]["report_date"]
    rows = []

    # A) Trucks that appeared on the weighbridge
    for truck in result["trucks"]:
        prod      = truck["production"]
        intervals = truck["trip_intervals"]
        flags     = truck["flags"] if truck["flags"] else ["NONE"]

        rows.append({
            "Report Date":                  report_date,
            "Truck Reg Number":             truck["horse_reg"],
            "Transporter":                  truck["transporter"] or "",
            "Auth Source":                  "Ghost (unregistered)" if truck["is_ghost"]
                                            else (truck["auth_source"] or ""),
            "In Tons Report":               "Yes",
            "Total Trips":                  prod["trip_count"],
            "Total Tons":                   round(prod["total_net_tonnes"], 2),
            "Avg Load (t)":                 round(prod["avg_net_tonnes"], 2),
            "Avg Time Between Trips (min)": intervals["avg_gap_min"],
            "Min Gap (min)":                intervals["min_gap_min"],
            "Max Gap (min)":                intervals["max_gap_min"],
            "Suspicious Flags":             " | ".join(flags),
        })

    # B) Authorised trucks absent from the weighbridge
    for source_key in ("impangele", "other"):
        for rec in result["checks"]["missing_fleet"][source_key]:
            rows.append({
                "Report Date":                  report_date,
                "Truck Reg Number":             rec.get("HORSE REG", ""),
                "Transporter":                  rec.get("TRANSPORTER", ""),
                "Auth Source":                  rec.get("AUTH_SOURCE", ""),
                "In Tons Report":               "No",
                "Total Trips":                  0,
                "Total Tons":                   0.00,
                "Avg Load (t)":                 None,
                "Avg Time Between Trips (min)": None,
                "Min Gap (min)":                None,
                "Max Gap (min)":                None,
                "Suspicious Flags":             "MISSING_FROM_WEIGHBRIDGE",
            })

    return pd.DataFrame(rows, columns=[
        "Report Date", "Truck Reg Number", "Transporter", "Auth Source",
        "In Tons Report", "Total Trips", "Total Tons", "Avg Load (t)",
        "Avg Time Between Trips (min)", "Min Gap (min)", "Max Gap (min)",
        "Suspicious Flags",
    ])


def write_summary_csv(result: dict, csv_path: str = DEFAULT_CSV_PATH) -> None:
    """
    Append this day's summary rows to the master CSV.
    Creates the file (with header) on first run; appends without header thereafter.
    Ensures the output directory exists before writing.
    """
    df = _build_summary_rows(result)

    out_dir = os.path.dirname(os.path.abspath(csv_path))
    os.makedirs(out_dir, exist_ok=True)

    file_exists = os.path.isfile(csv_path)
    df.to_csv(csv_path, mode="a", header=not file_exists, index=False)

    action = "Appended to" if file_exists else "Created"
    print(f"  {action} CSV → {csv_path}  ({len(df)} rows)")