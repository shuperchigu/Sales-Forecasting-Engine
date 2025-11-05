# config.py

from datetime import datetime
import os

# --- DATA LOADING PARAMETERS ---
START_DATE = "2023-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")
PROMO_MIN_CASHBACK = 11000

# --- PROPHET MODEL HYPERPARAMETERS ---
STAN_N_JOBS = int(os.getenv('STAN_N_JOBS', 8))
PROPHET_PARAMS = {
    'yearly_seasonality': False,
    'weekly_seasonality': False,
    'daily_seasonality': False,
    'seasonality_mode': 'additive',
    'changepoint_prior_scale': 0.06,
    'seasonality_prior_scale': 0.08,
    'uncertainty_samples': 0
}

MONTHLY_SEASONALITY = {
    'name': 'monthly',
    'period': 30.5,
    'fourier_order': 2
}

# --- FORECASTING LOGIC PARAMETERS ---
MIN_DATA_POINTS_FOR_FORECAST = 6  # მინიმუმ 6 თვის მონაცემი
STD_DEV_THRESHOLD = 0.01  # მინიმალური სტანდარტული გადახრა
LOGISTIC_GROWTH_THRESHOLD = 1.5 # ზღვარი, რომლის ზემოთაც ლოგისტიკური ზრდა ჩაირთვება
LOGISTIC_CAP_QUANTILE = 0.9
LOGISTIC_CAP_MULTIPLIER = 1.4

# --- EVALUATION PARAMETERS ---
CV_INITIAL = '180 days'
CV_PERIOD = '30 days'
CV_HORIZON = '30 days'
MIN_DATA_POINTS_FOR_CV = 10 # მინიმუმ 10 თვის მონაცემი შეფასებისთვის

# --- OPTIMIZATION PARAMETERS ---
TOTAL_BUDGET = 1_000_000

# --- OUTPUT PARAMETERS ---
OUTPUT_FOLDER = "output"
FINAL_KPI_FILENAME = "final_kpis.csv"
EVALUATION_FILENAME = "evaluation_metrics.csv"