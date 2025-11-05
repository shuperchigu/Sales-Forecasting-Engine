# generate_orders_csv.py

import pandas as pd
import os
import logging
from db import db_connection  

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_and_save_orders_data():
    """
    Fetches detailed orders data from the database using the existing connection
    and saves it to a CSV file.
    """
    logging.info("Starting to fetch detailed orders data...")
    
    sql_query = """
    select id as order_id,
           paid_date,
           user_id,
           product_id,
           barcode,
           product_name,
           round(price::numeric, 2) as price,
           round(cost::numeric, 2) as cost,
           round(beginning_quantity::numeric, 2) as beginning_quantity,
           round(refunded_quantity::numeric, 2) as refunded_quantity,
           round(quantity::numeric, 2) as quantity,
           round(cogs::numeric, 2) as cogs,
           round(revenue::numeric, 2) as revenue,
           card,
           transaction_payment_method,
           is_juridical,
           card_type,
           brand,
           supplier,
           mother_cat_name,
           subcategory,
           round(vendor_cashback::numeric, 2) as vendor_cashback,
           cashback_campaign_name,
           round(voucher_cashback::numeric, 2) as voucher_cashback,
           round(promotion_cashback::numeric, 2) as promotion_cashback
    from table_of_orders
    """
    
    try:
        logging.info("Using existing database connection to execute query...")
        
        df_orders = pd.read_sql(sql_query, db_connection)
        
        if df_orders.empty:
            logging.warning("No data returned from the orders query.")
            return

        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "orders_details.csv")
        
        logging.info(f"Query successful. Saving {len(df_orders)} rows to {output_path}...")
        df_orders.to_csv(output_path, index=False)
        logging.info("Successfully saved the data.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":

    fetch_and_save_orders_data()

