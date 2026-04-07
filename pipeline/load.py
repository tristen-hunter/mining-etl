import pandas as pd
import os

# 1. Path Configuration
YEAR, MONTH, DAY = "2026", "March", "19"

BASE_PATH = os.path.join("master", YEAR, MONTH, DAY)

# Thresholds from the original report
TARE_VAR_LIMIT = 500
MIN_CYCLE_MINUTES = 10
MIN_AVG_LOAD_TONS = 32

def load_data(base_path):
    # Filenames exactly as provided
    files = {
        "loading": "loading_list.csv", 
        "tons": "tons_report.csv", 
        "olf": "olf_bookings.csv"
    }
    data = {}
    for key, filename in files.items():
        file_path = os.path.join(base_path, filename)
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            # Standardize Registration for robust matching [cite: 85]
            if 'HORSE REG' in df.columns:
                df['HORSE REG'] = df['HORSE REG'].astype(str).str.replace(" ", "").str.upper()
            data[key] = df
        else:
            print(f"Warning: {filename} not found.")
            data[key] = None
    return data


import pandas as pd
import os

def run_full_audit(data):
    df_tons = data.get('tons')
    df_olf = data.get('olf')
    df_load = data.get('loading')

    if df_tons is None: return None

    # Standardize Timestamps
    df_tons['FIRST DATE & TIME'] = pd.to_datetime(df_tons['FIRST DATE & TIME'], dayfirst=True)
    df_tons['SECOND DATE & TIME'] = pd.to_datetime(df_tons['SECOND DATE & TIME'], dayfirst=True)
    
    # 1. Master Auth List
    auth_master = pd.concat([df_load, df_olf], ignore_index=True).drop_duplicates(subset=['HORSE REG'])
    
    # 2. Ghost & Missing [cite: 12, 17]
    tons_regs = set(df_tons['HORSE REG'].unique())
    auth_regs = set(auth_master['HORSE REG'].unique())
    ghosts = df_tons[~df_tons['HORSE REG'].isin(auth_regs)]
    no_shows = auth_master[~auth_master['HORSE REG'].isin(tons_regs)]

    # 3. Gap Analysis [cite: 39]
    df_tons['SCA_VAL'] = df_tons['SCALE NUMBER'].str.extract(r'(\d+)').astype(int)
    min_sca, max_sca = df_tons['SCA_VAL'].min(), df_tons['SCA_VAL'].max()
    gaps = sorted(list(set(range(min_sca, max_sca + 1)) - set(df_tons['SCA_VAL'])))

    # 4. Operational Metrics [cite: 45, 50, 55]
    df_tons['CYCLE_MIN'] = (df_tons['SECOND DATE & TIME'] - df_tons['FIRST DATE & TIME']).dt.total_seconds() / 60
    tare_vars = df_tons.groupby('HORSE REG')['FIRST MASS'].agg(lambda x: x.max() - x.min())
    avg_weights = df_tons.groupby('TRANSPORTER')['NET MASS'].mean() / 1000

    # Package all data for the PDF
    return {
        "summary": {
            "total_net_mass": df_tons['NET MASS'].sum(),
            "total_trips": len(df_tons),
            "ghost_count": len(ghosts),
            "missing_count": len(no_shows),
            "auth_count": len(auth_regs),
            "sca_range": (min_sca, max_sca)
        },
        "dataframes": {
            "ghosts": ghosts[['HORSE REG', 'SCALE NUMBER', 'TRANSPORTER']],
            "no_shows": no_shows[['FLEET NUMBER', 'HORSE REG', 'TRANSPORTER', 'DRIVER NAME']],
            "fast_cycles": df_tons[df_tons['CYCLE_MIN'] < 10],
            "high_tare": tare_vars[tare_vars >= 500],
            "underloaded": avg_weights[avg_weights < 32]
        },
        "gaps": gaps
    } 

# Execute
data_dict = load_data(BASE_PATH)
missing_fleet_df = run_full_audit(data_dict)