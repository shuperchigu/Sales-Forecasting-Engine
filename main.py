# main.py

import os
import logging
import warnings
from multiprocessing import get_context, cpu_count
from tqdm import tqdm
import pandas as pd
from database_writer import save_results_to_db

# --- Global Settings for Silence ---
# Disable warnings
warnings.filterwarnings("ignore")
# Disable all logging messages less critical than ERROR
logging.basicConfig(level=logging.ERROR)
logging.getLogger('prophet').setLevel(logging.ERROR)
logging.getLogger('cmdstanpy').setLevel(logging.ERROR)

# --- Set Prophet environment variables for silence ---
os.environ['CMDSTAN_NO_STDOUT'] = '1'
os.environ['CMDSTAN_NO_STDERR'] = '1'

import config
from query.LoadData import LoadData
from holidays import holidays as standard_holidays
from prophecy import forecast_one, calculate_kpis
from evaluate import evaluate_model_accuracy
from optimize import run_optimal_allocation

def main():
    print("🔮 Starting forecast process...")
    
    # --- 1. Load Data ---
    print("Step 1/5: Loading data...")
    df_raw = LoadData.get_sales_data(config.START_DATE, config.END_DATE)
    df_raw['ds'] = pd.to_datetime(df_raw['transaction_month'])
    df_raw['barcode'] = df_raw['barcode'].astype(str)

    additional_data = LoadData.get_additional_metrics()
    additional_data['barcode'] = additional_data['barcode'].astype(str)

    promo_holidays = LoadData.get_promo_campaigns(config.PROMO_MIN_CASHBACK)
    combined_holidays = pd.concat([standard_holidays, promo_holidays], ignore_index=True)

    # --- 2. Prepare Arguments for Parallel Processing ---
    print("Step 2/5: Preparing arguments for forecasting...")
    barcode_groups = list(df_raw.groupby('barcode'))
    
    forecast_args = []
    for barcode, group_df in barcode_groups:
        median_add_3m = 0.0
        if not additional_data.empty:
            match = additional_data[additional_data['barcode'] == barcode]
            if not match.empty:
                val = match['median_add_3m'].iloc[0]
                median_add_3m = 0.0 if pd.isna(val) else float(val)
        
        forecast_args.append((barcode, group_df, combined_holidays, median_add_3m))

    # --- 3. Run Forecasting in Parallel ---
    print(f"Step 3/5: Running forecast for {len(forecast_args)} products...")
    # Use 'spawn' context for better cross-platform compatibility
    with get_context("spawn").Pool(processes=max(1, cpu_count() - 1)) as pool:
        forecast_results = list(tqdm(pool.imap(forecast_one, forecast_args), total=len(forecast_args), desc="Forecasting"))

    forecast_df = pd.DataFrame([res[:2] for res in forecast_results], columns=['barcode', 'forecastedADD'])
    forecast_df['barcode'] = forecast_df['barcode'].astype(str)

    # --- 4. Calculate KPIs and Finalize ---
    print("Step 4/5: Calculating final KPIs...")
    final_df = calculate_kpis(df_raw, forecast_df, additional_data)
    
    # --- (Optional) Run Optimization ---
    # final_df = run_optimal_allocation(final_df)

    # --- 5. Save Results ---
    print("Step 5/5: Saving results...")
    if not os.path.exists(config.OUTPUT_FOLDER):
        os.makedirs(config.OUTPUT_FOLDER)
    
    output_path = os.path.join(config.OUTPUT_FOLDER, config.FINAL_KPI_FILENAME)
    
    # Format barcode for Excel to prevent scientific notation
    final_df['barcode'] = "'" + final_df['barcode'].astype(str)
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"✅ Process finished successfully! Results saved to {output_path}")

    save_results_to_db(final_df, 'veli_prophet_results')
    print(f"✅ Process finished successfully!")

    # --- (Optional) Run Evaluation on a sample of products ---
    # print("\n🔬 Starting optional evaluation process...")
    # sample_barcodes = forecast_args[:10] # Evaluate first 10 products
    # all_metrics = []
    # for args in tqdm(sample_barcodes, desc="Evaluating Models"):
    #     barcode, group_df, holidays_df, _ = args
    #     metrics = evaluate_model_accuracy(barcode, group_df, holidays_df)
    #     if metrics is not None:
    #         all_metrics.append(metrics)
    #
    # if all_metrics:
    #     evaluation_df = pd.concat(all_metrics, ignore_index=True)
    #     eval_path = os.path.join(config.OUTPUT_FOLDER, config.EVALUATION_FILENAME)
    #     evaluation_df.to_csv(eval_path, index=False)
    #     print(f"✅ Evaluation metrics saved to {eval_path}")

if __name__ == "__main__":
    main()