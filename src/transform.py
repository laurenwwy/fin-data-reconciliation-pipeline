import pandas as pd
import numpy as np
import logging

# We set up logging so that when this runs automatically, we have a trail of what happened.
# In Finance/Audit, "print()" statements aren't enough. We need formal logs.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#1.  That line configures Python’s logging system — basically telling Python how you want log messages to look and what level of messages to show.
# This line sets up global logging behavior for your script.
# Logging levels (from lowest to highest):DEBUG,INFO, WARNING,ERROR, CRITICAL
# By choosing INFO, you are saying:“Show me INFO, WARNING, ERROR, and CRITICAL — but hide DEBUG.”
# 2. format='%(asctime)s - %(levelname)s - %(message)s'
# This defines how each log line will look.
# Each %(...)s is a placeholder.
# Meaning of each placeholder:
# %(asctime)s → timestamp (when the log happened)
# %(levelname)s → INFO, WARNING, ERROR, etc.
# %(message)s → the actual log text you wrote
# eg. logging.info("File loaded successfully") : 2026-05-09 20:26:45,123 - INFO - File loaded successfully

def reconcile_transactions(ledger_path: str, clearing_path: str) -> pd.DataFrame:
    """Reads internal and external data, reconciles them, and identifies breaks."""
    logging.info("Starting reconciliation process...")
    
    df_ledger = pd.read_csv(ledger_path)
    df_clearing = pd.read_csv(clearing_path)

    # Outer join to find missing records on either side
    merged_df = pd.merge(
        df_ledger, df_clearing, 
        on='transaction_id', how='outer', 
        suffixes=('_internal', '_external')
    )

    # Flag discrepancies
    # np.where is a Numpy function that acts like an IF/ELSE statement, 
    # but it processes the entire column instantly (vectorization) instead of looping row-by-row.
    # Logic: IF internal amount != external amount OR internal amount is missing OR external amount is missing 
    # -> THEN True (it's a break) ELSE False.
    merged_df['is_break'] = np.where(
        (merged_df['amount_internal'] != merged_df['amount_external']) |
        (merged_df['amount_internal'].isna()) |
        (merged_df['amount_external'].isna()), 
        True, False
    )

    # Categorize the break reason
    # We create a custom Python function to figure out exactly *why* it broke.
    def categorize_break(row):
        if not row['is_break']:
            return 'Matched'
        elif pd.isna(row['amount_internal']):
            return 'Missing in Internal Ledger'
        elif pd.isna(row['amount_external']):
            return 'Missing in External Clearing'
        else:
            return 'Amount Mismatch'
    # We use .apply() to run our custom function down the entire dataframe, axis=1 means apply it row-by-row.
    merged_df['break_reason'] = merged_df.apply(categorize_break, axis=1)

    # OR a much faster way using np.select (which is like a vectorized IF/ELSE):Instead of looping through rows, we let pandas evaluate entire columns at once.
#     merged_df['break_reason'] = np.select(
#     [
#         merged_df['is_break'] == False,
#         merged_df['amount_internal'].isna(),
#         merged_df['amount_external'].isna()
#     ],
#     [
#         'Matched',
#         'Missing in Internal Ledger',
#         'Missing in External Clearing'
#     ],
#     default='Amount Mismatch'
# )
    breaks_count = merged_df['is_break'].sum()
    logging.info(f"Reconciliation complete. Found {breaks_count} breaks.")
    
    return merged_df