# prophecy.py

import pandas as pd
import numpy as np
import io
import contextlib
import logging
from datetime import datetime

import config
from utils import create_prophet_model

# Silence Prophet logs (already handled globally, but good practice per module)
logging.getLogger('prophet').setLevel(logging.ERROR)
logging.getLogger('cmdstanpy').setLevel(logging.ERROR)


def forecast_one(args):
    """
    Runs Prophet forecast for a single barcode.
    Returns a tuple: (barcode, forecast_value, start_time, end_time)
    """
    barcode, group, combined_holidays, median_add_3m = args
    start_time = datetime.now()

    try:
        # Redirect stderr to hide cmdstanpy's initial output
        with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
            
            # --- PRE-FORECAST CHECKS ---
            if median_add_3m == 0 or pd.isna(median_add_3m):
                return (barcode, 0.0, start_time, datetime.now())

            df = group.copy()
            df['ds'] = pd.to_datetime(df['transaction_month'])
            
            # Use the best available demand column
            demand_cols = ['rolling_median_add', 'avg_daily_demand', 'avg_daily_demand_real']
            y_col = next((col for col in demand_cols if col in df.columns and df[col].notna().any()), None)
            
            if not y_col:
                return (barcode, median_add_3m, start_time, datetime.now())
            df['y'] = df[y_col]

            # Aggregate data by month
            df = df.groupby('ds', as_index=False).agg(
                y=('y', 'mean'),
                in_stock_days=('in_stock_days', 'mean')
            ).dropna()

            # --- VALIDATION ---
            if len(df) < config.MIN_DATA_POINTS_FOR_FORECAST or df['y'].sum() == 0:
                return (barcode, median_add_3m, start_time, datetime.now())
            
            if df['y'].std() < config.STD_DEV_THRESHOLD:
                 return (barcode, median_add_3m, start_time, datetime.now())

            # --- MODEL CONFIGURATION ---
            use_logistic = (df['y'].max() / df['y'].mean()) > config.LOGISTIC_GROWTH_THRESHOLD
            growth_type = 'logistic' if use_logistic else 'linear'
            
            model = create_prophet_model(combined_holidays, growth_type)
            model.add_regressor('in_stock_days')
            
            fit_df = df[['ds', 'y', 'in_stock_days']].drop_duplicates(subset=['ds'])

            if use_logistic:
                cap_val = df['y'].quantile(config.LOGISTIC_CAP_QUANTILE) * config.LOGISTIC_CAP_MULTIPLIER
                fit_df['cap'] = cap_val
                fit_df['floor'] = 0.01

            # --- FITTING AND PREDICTING ---
            model.fit(fit_df, n_jobs=config.STAN_N_JOBS)

            future = model.make_future_dataframe(periods=1, freq='MS')
            future['in_stock_days'] = 30
            if use_logistic:
                future['cap'] = cap_val
                future['floor'] = 0.01

            forecast = model.predict(future)
            
            # --- POST-PROCESSING ---
            forecast_next_month = forecast[forecast['ds'] > df['ds'].max()]
            if forecast_next_month.empty:
                return (barcode, median_add_3m, start_time, datetime.now())

            forecasted_add = forecast_next_month['yhat'].iloc[0]
            
            if median_add_3m > 0 and forecasted_add < median_add_3m / 2:
                print(f"INFO: Barcode {barcode} forecast ({forecasted_add:.4f}) was too low compared to median ({median_add_3m:.4f}). Using median instead.")
                final_forecast = median_add_3m
            else:
                final_forecast = forecasted_add

            if not np.isfinite(final_forecast) or final_forecast < 0:
                final_forecast = median_add_3m

            return (barcode, round(float(final_forecast), 4), start_time, datetime.now())

    except Exception as e:
        # Log the error for debugging, but don't stop the whole process
        print(f"WARNING: Forecast for barcode {barcode} failed: {e}. Defaulting to median_add_3m.")
        return (barcode, median_add_3m, start_time, datetime.now())

def calculate_kpis(df_raw: pd.DataFrame, forecast_df: pd.DataFrame, additional_data: pd.DataFrame) -> pd.DataFrame:
    """Calculates final KPIs, merges all data sources, and adds business metrics."""
    from utils import safe_mean_filtered
    
    if not pd.api.types.is_datetime64_any_dtype(df_raw['ds']):
        df_raw['ds'] = pd.to_datetime(df_raw['ds'])

    demand_cols = ['rolling_median_add', 'avg_daily_demand', 'avg_daily_demand_real']
    demand_col = next((col for col in demand_cols if col in df_raw.columns), None)
    if not demand_col:
        raise ValueError(f"No demand column found in raw data. Available: {df_raw.columns.tolist()}")

    filtered = df_raw[df_raw[demand_col] > 0]
    agg = filtered.groupby('barcode').agg(
        averageADD=(demand_col, lambda x: round(x[x > 0].mean(), 4)),
        DSI=('dsi', lambda x: safe_mean_filtered(x)),
        GMROI=('gmroi', lambda x: safe_mean_filtered(x)),
    )

    result = agg.reset_index().merge(forecast_df, on='barcode', how='left')
    
    if additional_data is not None and not additional_data.empty:
        # დარწმუნდით, რომ ყველა საჭირო ველი არსებობს additional_data-ში
        merge_cols = [col for col in ['barcode', 'product_name', 'mother_cat_name', 'subcategory', 
                                      'supplier_name', 'brand', 'in_stock', 'price', 'cost', 
                                      'items_sold_3m', 'median_add_3m'] 
                      if col in additional_data.columns]
        result = result.merge(additional_data[merge_cols], on='barcode', how='left')
    
    # --- ახალი ველების გამოთვლა ---
    # ვავსებთ ცარიელ მნიშვნელობებს, რათა თავიდან ავიცილოთ შეცდომები
    for col in ['items_sold_3m', 'price', 'cost', 'in_stock', 'forecastedADD']:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce').fillna(0)

    # 1. Revenue და COGS (ბოლო 3 თვის)
    result['revenue_last_3m'] = result['items_sold_3m'] * result['price']
    result['COGS_last_3m'] = result['items_sold_3m'] * result['cost']
    
    # 2. შესაძენი რაოდენობის ღირებულება
    # ფორმულა: (პროგნოზირებული მოთხოვნა 30 დღეზე * ფასი) - (მარაგის ღირებულება)
    result['purchase_recommendation_cost'] = (result['forecastedADD'] * 30 * result['cost']) - (result['in_stock'] * result['cost'])
    
    # პირობა: თუ 0-ზე ნაკლებია, გახდეს 0
    result['purchase_recommendation_cost'] = result['purchase_recommendation_cost'].clip(lower=0)
    
    # --- საბოლოო ველების სია ---
    # ვცვლით 'items_sold_3m'-ის სახელს უფრო გასაგებით
    result.rename(columns={'items_sold_3m': 'items_sold_last_3m'}, inplace=True)
    
    final_cols = [
        'barcode', 'product_name', 'mother_cat_name', 'subcategory', 'supplier_name', 'brand', 
        'in_stock', 'cost', 'price', 'DSI', 'GMROI', 
        'forecastedADD', 'median_add_3m',
        'items_sold_last_3m', 'revenue_last_3m', 'COGS_last_3m', # <-- ახალი ბიზნეს ველები
        'purchase_recommendation_cost' # <-- ახალი სარეკომენდაციო ველი
    ]
    
    # დარწმუნდით, რომ ყველა სვეტი არსებობს DataFrame-ში
    for col in final_cols:
        if col not in result.columns:
            result[col] = 0.0

    return result[final_cols].copy()