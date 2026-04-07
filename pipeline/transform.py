# import os
# import pandas as pd

# def get_best_file(file_paths, anchor_col):
#     best_df = None
#     max_valid_entries = -1
#     winning_path = ""

#     for path in file_paths:
#         try:
#             # 1. Determine file type and load raw to find headers
#             if path.endswith('.csv'):
#                 raw_df = pd.read_csv(path, header=None).fillna("")
#             else:
#                 raw_df = pd.read_excel(path, header=None).fillna("")
            
#             # 2. Find header row dynamically (Issue #1 Fix)
#             header_idx = -1
#             anchor_clean = anchor_col.lower().strip(".") # Normalize the search term
#             for i, row in raw_df.iterrows():
#                 # Normalize the cell values for comparison
#                 if any(anchor_clean in str(val).lower().strip(".") for val in row):
#                     header_idx = i
#                     break

    
            
#             if header_idx == -1: continue 

#             # 3. Reload with correct header
#             if path.endswith('.csv'):
#                 df = pd.read_csv(path, skiprows=header_idx)
#             else:
#                 df = pd.read_excel(path, skiprows=header_idx)
            
#             # 4. Clean column names
#             df.columns = df.columns.str.strip()
            
#             # 5. Count UNIQUE valid entries (Issue #1 Fix) 
#             if anchor_col in df.columns:
#                 # nunique() ignores NaNs and prevents "Total" rows from inflating count [cite: 3, 7]
#                 valid_count = df[anchor_col].replace('', pd.NA).dropna().nunique()
                
#                 # Winning logic: Highest count, then latest timestamp
#                 if valid_count > max_valid_entries:
#                     max_valid_entries = valid_count
#                     best_df = df
#                     winning_path = path
#                 elif valid_count == max_valid_entries and max_valid_entries > 0:
#                     if os.path.getmtime(path) > os.path.getmtime(winning_path):
#                         best_df = df
#                         winning_path = path
#         except Exception as e:
#             print(f"Error processing {path}: {e}")
#             continue

#     return best_df, winning_path

# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# TARGET_DIR = os.path.join(BASE_DIR, "archive\\2026")
# print(TARGET_DIR)

# MASTER_DIR = os.path.join(os.path.dirname(__file__), '..', 'master')
# os.makedirs(MASTER_DIR, exist_ok=True)

# # print(BASE_DIR)
# # print(TARGET_DIR)


# for month in os.listdir(TARGET_DIR):
#     # Get Path
#     month_path = os.path.join(TARGET_DIR, month)

#     # Check that the Dir exists, skips if not
#     if not os.path.isdir(month_path):
#         continue

#     # print(month_path)
#     for day in os.listdir(month_path):
#         # Get Path
#         day_path = os.path.join(month_path, day)

#         # Check that the Dir exists, skips if not
#         if not os.path.isdir(day_path):
#             continue

#         candidates = {'loading_list': [], 'tons_report': [], 'olf': []}

#         print(f"\nProcessing: {month}/{day}")

#         #  1. CATEGORIZE CANDIDATES
#         for file in os.listdir(day_path):
#             if not file.endswith(('.xlsx', '.xls', '.csv')): continue
            
#             path = os.path.join(day_path, file)
#             name = file.lower()

#             if "loading" in name: candidates['loading_list'].append(path)
#             elif "tons" in name: candidates['tons_report'].append(path)
#             elif "olf" in name: candidates['olf'].append(path)


#         # 2. VALIDATION & SELECTION LOGIC
#         # Find the best Fleet List using "Driver ID" as the anchor
#         best_loading_df, loading_path = get_best_file(candidates['loading_list'], "Driver ID")
        
#         # Find the best OLF Booking using "ID" as the anchor to catch "ID/Passport" 
#         best_olf_df, olf_path = get_best_file(candidates['olf'], "ID")
        
#         # Consolidate Tons Reports (Issue #3 Fix - Remove Totals)
#         tons_dfs = []
#         for path in candidates['tons_report']:
#             try:
#                 # Assuming "Tons" or "Net" is a safe anchor for these reports
#                 df_temp, _ = get_best_file([path], "Tons") 
#                 if df_temp is not None:
#                     # Filter out rows where "Tons" might be a string like "Total"
#                     df_temp = df_temp[pd.to_numeric(df_temp.iloc[:, 0], errors='coerce').notnull()]
#                     tons_dfs.append(df_temp)
#             except Exception as e:
#                 print(f"Skipping report {path}: {e}")

#         # 3. PERSIST TO MASTER
#         # Define the output folder for this specific day
#         output_day_dir = os.path.join(MASTER_DIR, month, day)
#         os.makedirs(output_day_dir, exist_ok=True)

#         if best_loading_df is not None:
#             # Standardize column naming before saving
#             best_loading_df.to_csv(os.path.join(output_day_dir, "master_loading_list.csv"), index=False)
#             print(f"Saved Master Loading List from: {os.path.basename(loading_path)}")

#         if best_olf_df is not None:
#             best_olf_df.to_csv(os.path.join(output_day_dir, "master_olf_bookings.csv"), index=False)
#             print(f"Saved Master OLF from: {os.path.basename(olf_path)}")

#         if tons_dfs:
#             master_tons = pd.concat(tons_dfs, ignore_index=True)
#             master_tons.to_csv(os.path.join(output_day_dir, "consolidated_tons.csv"), index=False)
#             print(f"Consolidated {len(tons_dfs)} tons reports.")

#         # 4. FINAL CLEANUP & LOGGING
#         if best_loading_df is None and best_olf_df is None and not tons_dfs:
#             print(f"!!! No valid data found for {month}/{day}")
#             print(candidates)
#         else:
#             print(f"Successfully processed {month}/{day}")