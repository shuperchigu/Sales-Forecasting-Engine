# utils.py

import pandas as pd
from prophet import Prophet
import config

# utils.py
def create_prophet_model(holidays_df: pd.DataFrame, growth_type: str = 'linear') -> Prophet:
    """
    Creates a Prophet model with standardized parameters from the config file.
    """
    model = Prophet(
        growth=growth_type,
        holidays=holidays_df,
        stan_backend='CMDSTANPY',  
        **config.PROPHET_PARAMS
    )
    model.add_seasonality(**config.MONTHLY_SEASONALITY)
    return model

def safe_mean_filtered(series: pd.Series, max_val: int = 10000) -> float:
    """
    Calculates the mean of a series after filtering out zeros and outliers.
    """
    valid = series[(series > 0) & (series < max_val)]

    return valid.mean() if not valid.empty else 0.0
