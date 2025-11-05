# evaluate.py

import pandas as pd
from prophet.diagnostics import cross_validation, performance_metrics
import logging
from typing import Union
import config
from utils import create_prophet_model

logging.getLogger('prophet').setLevel(logging.ERROR)
logging.getLogger('cmdstanpy').setLevel(logging.ERROR)

def evaluate_model_accuracy(barcode: str, group: pd.DataFrame, combined_holidays: pd.DataFrame) -> Union[pd.DataFrame, None]:
    """
    Performs cross-validation for a single product and returns performance metrics.
    """
    df = group.copy()
    df['ds'] = pd.to_datetime(df['transaction_month'])
    
    demand_cols = ['rolling_median_add', 'avg_daily_demand', 'avg_daily_demand_real']
    y_col = next((col for col in demand_cols if col in df.columns and df[col].notna().any()), None)
    
    if not y_col:
        print(f"INFO: No suitable demand column for evaluation of barcode {barcode}")
        return None

    df['y'] = df[y_col]
    df = df[['ds', 'y']].dropna().drop_duplicates(subset=['ds'])

    if len(df) < config.MIN_DATA_POINTS_FOR_CV:
        print(f"INFO: Not enough data for CV for barcode {barcode} (has {len(df)}, needs {config.MIN_DATA_POINTS_FOR_CV}).")
        return None

    try:
        # Use the same model creation logic for consistency
        model = create_prophet_model(combined_holidays, growth_type='linear')
        
        # NOTE: For evaluation, complex logic like logistic growth can make CV unstable.
        # We stick to linear to get a baseline performance metric.
        model.fit(df[['ds', 'y']])

        df_cv = cross_validation(
            model, 
            initial=config.CV_INITIAL,
            period=config.CV_PERIOD,
            horizon=config.CV_HORIZON,
            disable_tqdm=True,
            error_score='raise'
        )
        
        df_perf = performance_metrics(df_cv)
        print(f"SUCCESS: CV completed for barcode {barcode}.")
        df_perf['barcode'] = barcode
        return df_perf

    except Exception as e:
        print(f"WARNING: Cross-validation failed for barcode {barcode}: {e}")
        return None