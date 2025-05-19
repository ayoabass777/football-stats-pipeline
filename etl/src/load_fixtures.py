import logging
import etl.src.config as config
import pandas as pd
from sqlalchemy import create_engine, text

DB_CONFIG = config.DB_CONFIG

logging.basicConfig(filename='load_to_db_logs.txt',
                    level= logging.INFO,
                    format= "%(asctime)s - %(levelname)s :%(message)s",
                    datefmt= "%Y-%m-%d %H:%M:%S"
                    )

def load_to_db (parquet_file: str,
               table_name: str = "raw_fixtures",
               if_exists: str = "append"):
     
    """
    Loads a DataFrame into a PostgreSQL table.

    Parameters:
      parquet_file: str  – path to the parquet file to load (required)
      table_name:   str  – target DB table (default "raw_fixtures")
      if_exists:    str  – 'append', 'replace', or 'fail' (default 'append')
    """
     
    if parquet_file is None:
        raise ValueError("parquet_file must be provided")

    db_uri = (
        f"postgresql+psycopg2://"
        f"{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
        f"{DB_CONFIG['dbname']}")
    
    try:
        # 1) Read parquet
        df = pd.read_parquet(parquet_file)
        logging.info("Parquet file loaded")

        # 2) Prepare engine
        engine = create_engine(db_uri)
        schema = "raw"

        # 3) Open a transaction
        with engine.begin() as conn:
            # 3a) Check if schema exists else create
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

            #3b) truncate table if table exists else append table to schema
            exists = conn.execute(text(
                f"SELECT EXISTS ("
                f"  SELECT 1 FROM information_schema.tables "
                f"  WHERE table_schema = '{schema}' AND table_name = '{table_name}'"
                f");"
            )).scalar()

            if exists:
                conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name};"))
                logging.info(f"Truncated table {schema}.{table_name}")
            
            else:
                logging.info(f"Table {schema}.{table_name} does not exist, skipping truncate")

            # 3c) Bulk load via pandas
            df.to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists=if_exists,
                index=False
                )
            logging.info(f"Loaded {len(df)} rows into {schema}.{table_name}")

    except Exception as e:
        logging.error(f"Failed to load data into table '{table_name}': {e}")
        raise 


if __name__ == "__main__":
    load_to_db("./data/cleaned_fixtures.parquet")



