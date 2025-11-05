# database_writer.py

import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# .env ფაილიდან ცვლადების ჩატვირთვა
load_dotenv()

def get_sqlalchemy_engine():
    """
    Creates an SQLAlchemy engine from environment variables for efficient data writing.
    """
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_password, db_host, db_name]):
        print("ERROR: Database environment variables are not set.")
        return None

    # SQLAlchemy კავშირის URL ფორმატი PostgreSQL-სთვის
    db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        engine = create_engine(db_url)
        # შევამოწმოთ კავშირი
        with engine.connect() as connection:
            print("INFO: SQLAlchemy engine created and connection successful.")
        return engine
    except Exception as e:
        print(f"ERROR: Could not create SQLAlchemy engine: {e}")
        return None

def save_results_to_db(df: pd.DataFrame, table_name: str):
    """
    Saves a DataFrame to a PostgreSQL table, completely replacing the table on each run.
    """
    print(f"INFO: Preparing to save results to database table '{table_name}'...")
    
    engine = get_sqlalchemy_engine()
    if engine is None:
        print("WARNING: Could not get database engine. Skipping database save.")
        return

    # --- მნიშვნელოვანი: Excel-ისთვის დამატებული აპოსტროფის მოშორება ---
    # `main.py`-ში ბარკოდს ვუმატებთ "'" პრეფიქსს. ეს ბაზაში არ უნდა შევინახოთ.
    df_to_save = df.copy()
    if 'barcode' in df_to_save.columns:
        df_to_save['barcode'] = df_to_save['barcode'].astype(str).str.lstrip("'")

    try:
        # ვიყენებთ pandas-ის to_sql მეთოდს. if_exists='replace' წაშლის ძველ ცხრილს
        # და მის ადგილას შექმნის ახალს ამ DataFrame-ის სტრუქტურით.
        df_to_save.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi' # ეფექტურია დიდი რაოდენობის მონაცემების ჩაწერისთვის
        )
        print(f"SUCCESS: {len(df_to_save)} rows saved to table '{table_name}'. The old table was replaced.")
    except Exception as e:
        print(f"ERROR: Failed to save data to database table '{table_name}': {e}")
    finally:
        # გავათავისუფლოთ რესურსები
        if engine:
            engine.dispose()