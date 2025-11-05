# db.py

import os
import sys
import psycopg2
from dotenv import load_dotenv

# .env ფაილიდან ცვლადების ჩატვირთვა
load_dotenv()

def get_db_connection():
    """
    Establishes a secure connection to the database using environment variables.
    Exits the program if required variables are not set.
    """
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_password, db_host, db_name]):
        print("Error: Database environment variables (DB_USER, DB_PASSWORD, DB_HOST, DB_NAME) must be set.", file=sys.stderr)
        sys.exit(1)

    try:
        conn = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error: Could not connect to the database: {e}", file=sys.stderr)
        sys.exit(1)

# ერთი კავშირის ობიექტი, რომელსაც მოდულები გამოიყენებენ
db_connection = get_db_connection()