import pandas as pd
import json
from datetime import datetime

def generate_mining_summary(file_path):
    # Load the dataset
    df = pd.read_csv(file_path)
    
    # --- 1. CLEANING & NORMALIZATION ---
    
    def normalize_transporter(name):
        if pd.isna(name): return "UNKNOWN"
        name = str(name).upper().strip()
        
        # Mapping variations to Key Transporters
        if "IMPANGELE" in name: return "IMPANGELE"
        if "I-POWER" in name or "IPOWER" in name: return "I-POWER TRUCKING"
        if "CBS" in name: return "CBS TRANSPORT"
        if "COMO" in name: return "COMO TRANS"
        if "PC" in name: return "PC'S TRANSPORT"
        if "JD" in name: return "JD INVESTMENTS"
        if "SWIFT" in name or "SWIIFT" in name: return "SWIFT"
        if "FREIGHTLINK" in name: return "FREIGHTLINK"
        if "ISANKU" in name: return "ISANKU"
        if "THIRDGEN" in name: return "THIRDGEN"
        return name

    df['Transporter_Cleaned'] = df['Transporter'].apply(normalize_transporter)
    
    # Ensure numeric types
    df['Total Tons'] = pd.to_numeric(df['Total Tons'], errors='coerce').fillna(0)
    df['Total Trips'] = pd.to_numeric(df['Total Trips'], errors='coerce').fillna(0)
    df['Avg Time Between Trips (min)'] = pd.to_numeric(df['Avg Time Between Trips (min)'], errors='coerce')
    df['Report Date'] = pd.to_datetime(df['Report Date'], dayfirst=True, errors='coerce')

    # --- 2. CALCULATING METRICS ---

    # 1. Total tons moved
    total_tons = df['Total Tons'].sum()

    # 2. Average coal mined per day
    unique_days = df['Report Date'].nunique()
    avg_per_day = total_tons / unique_days if unique_days > 0 else 0

    # 3. Average truck turn around (Avg Time Between Trips)
    # Excludes nulls (trucks with only 1 trip)
    avg_turnaround = df['Avg Time Between Trips (min)'].mean()

    # 4. Average nett mass (Average load per trip)
    total_trips = df['Total Trips'].sum()
    avg_nett_mass = total_tons / total_trips if total_trips > 0 else 0

    # 5. Loads broken down by Key Transporter
    transporter_loads = df.groupby('Transporter_Cleaned')['Total Trips'].sum().to_dict()
    # Remove "UNKNOWN" if it has 0 loads
    transporter_loads = {k: int(v) for k, v in transporter_loads.items() if v > 0}

    # 6. Trucks that moved coal (Active)
    moved_coal = df[df['Total Tons'] > 0]['Truck Reg Number'].unique().tolist()

    # 7. Trucks that didn't move coal (Inactive)
    # Defined as trucks present in the data but with 0 tons for the whole period
    all_trucks = df['Truck Reg Number'].dropna().unique().tolist()
    did_not_move = [t for t in all_trucks if t not in moved_coal]

    # --- 3. CONSTRUCTING THE JSON ---
    
    report_data = {
        "report_metadata": {
            "period_days": unique_days,
            "total_trips_recorded": int(total_trips)
        },
        "kpis": {
            "total_tons_moved": round(total_tons, 2),
            "avg_coal_mined_per_day": round(avg_per_day, 2),
            "avg_truck_turnaround_mins": round(avg_turnaround, 2) if not pd.isna(avg_turnaround) else 0,
            "avg_nett_mass_per_load": round(avg_nett_mass, 2)
        },
        "transporter_breakdown": transporter_loads,
        "truck_analysis": {
            "active_trucks_count": len(moved_coal),
            "inactive_trucks_count": len(did_not_move),
            "list_active_trucks": moved_coal,
            "list_inactive_trucks": did_not_move
        }
    }
    
    return report_data

if __name__ == "__main__":
    FILE = r"C:\Users\trist\dev\mining-etl\DB\reports_summary.csv"
    data_for_pdf = generate_mining_summary(FILE)
    print(json.dumps(data_for_pdf, indent=4))