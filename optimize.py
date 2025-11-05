# optimize.py

import pandas as pd
from pulp import LpMaximize, LpProblem, LpVariable, lpSum, PULP_CBC_CMD
import config

def run_optimal_allocation(kpi_df: pd.DataFrame) -> pd.DataFrame:
    """
    Runs budget optimization. This is a placeholder and requires
    'Bmax' and 'R_index' columns to be defined in the input DataFrame.
    """
    df = kpi_df.copy()

    # --- This logic is illustrative. Bmax and R_index must be calculated first ---
    # For demonstration, let's create dummy columns
    if 'cost' in df.columns and 'forecastedADD' in df.columns:
        df['Bmax'] = df['cost'] * df['forecastedADD'] * 30 * 2  # Example: 2 months of stock
        df['R_index'] = (df['price'] - df['cost']) / df['cost'] # Example: simple margin
    else:
        print("WARNING: 'cost' or 'forecastedADD' not in DataFrame. Skipping optimization.")
        kpi_df['Optimal_spent'] = 0.0
        return kpi_df
    # --- End of illustrative logic ---

    df = df[(df['Bmax'] > 0) & (df['R_index'].notnull()) & (df['R_index'] > 0)].copy()

    if df.empty:
        print("WARNING: No products available for optimization.")
        kpi_df['Optimal_spent'] = 0.0
        return kpi_df

    model = LpProblem(name="optimal-budget-allocation", sense=LpMaximize)
    
    # Define variables
    x = {
        row['barcode']: LpVariable(name=f"x_{row['barcode']}", lowBound=0, upBound=row['Bmax'])
        for _, row in df.iterrows()
    }

    # Set objective function
    model += lpSum(
        x[row['barcode']] * row['R_index'] for _, row in df.iterrows()
    ), "Total_ROI"

    # Set budget constraint
    model += lpSum(x.values()) <= config.TOTAL_BUDGET, "Budget_Limit"

    # Solve the problem
    model.solve(PULP_CBC_CMD(msg=0)) # msg=0 silences the solver output

    # Map results back
    allocation = {barcode: var.value() for barcode, var in x.items()}
    kpi_df['Optimal_spent'] = kpi_df['barcode'].map(allocation).fillna(0.0)
    
    print("SUCCESS: Optimization completed.")
    return kpi_df