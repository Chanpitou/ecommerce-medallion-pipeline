import snowflake.connector
import os
import csv
from src.utils.config import SNOWFLAKE_CONFIG
import logging

logging.basicConfig(format='%(asctime)s: %(levelname)s - %(message)s', level=logging.INFO)

# Raw data sources to use
DATA_SOURCES = {
    "brazilian": {
        "path": "data/raw/Brazilian_E-Commerce_Public_Dataset_by_Olist",
        "files": [
            "olist_customers_dataset.csv",
            "olist_orders_dataset.csv",
            "olist_order_items_dataset.csv",
            "olist_products_dataset.csv",
            "olist_sellers_dataset.csv"
        ]
    },
    "amazon": {
        "path": "data/raw/Amazon_Product_Dataset",
        "files": [
            "amazon_audio_video.csv",
            "amazon_camra.csv",
            "amazon_car_accessories.csv",
            "amazon_laptop.csv",
            "amazon_men.csv",
            "amazon_men_shoe.csv",
            "amazon_mobile.csv",
            "amazon_movies.csv",
            "amazon_toys_1.csv"
        ]
    }
}

# Snowflake connection
def get_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
        role=SNOWFLAKE_CONFIG["role"],
    )

def create_table_from_csv(cursor, table_name, file_path):
    with open(file_path, "r", encoding="latin-1") as file:
        logging.info(f"Reading file {file_path}")
        reader = csv.reader(file)
        header = next(reader)

        columns = []

        for col in header:
            col_clean = col.replace('"', '').strip()
            columns.append(f'"{col_clean}" VARCHAR')

        create_tbl_query = f"""
        CREATE OR REPLACE TABLE {table_name} (
            {', '.join(columns)}
        );
        """
        try:
            logging.info(f"Creating {table_name} table...")
            cursor.execute(create_tbl_query)

            logging.info(f"Successfully create {table_name} table.")
        except Exception as e:
            logging.error(f"{e}: unable to create table {table_name}")

def upload_to_stage(cursor, file_path):
    try:
        logging.info(f"Uploading {file_path} to stage")
        cursor.execute(
            f"""PUT file://{os.path.abspath(file_path)}
                @internal_stage 
                AUTO_COMPRESS=TRUE 
                OVERWRITE=TRUE
            """
            # Compressing file into .gz when in stage
        )
    except Exception as e:
        logging.error(f"{e}: unable to upload {file_path} to stage")

def copy_csv_into_table(cursor, table_name, file_path):
    file_name = os.path.basename(file_path)
    try:
        cursor.execute(
            f"""
            COPY INTO {table_name}
            FROM @internal_stage/{file_name}.gz
            FILE_FORMAT = (FORMAT_NAME = bronze.csv_format)
            ON_ERROR = 'CONTINUE';
        """
        )
    except Exception as e:
        logging.error(f"{e}: unable to copy {file_name} to stage")


def copy_json_into_table(cursor, table_name, file_path):
    file_name = os.path.basename(file_path)

    cursor.execute(
        f"""
        COPY INTO {table_name}(raw_data)
        FROM @internal_stage/{file_name}.gz
        FILE_FORMAT = (FORMAT_NAME = bronze.json_format)
        ON_ERROR = 'CONTINUE';
    """
    )

if __name__ == "__main__":
    try:
        logging.info("Connecting SnowFlake database...")
        connection = get_connection()
        cursor = connection.cursor()

        for source in DATA_SOURCES.values():
            folder_path = source["path"]
            for file in source["files"]:
                file_path = os.path.join(folder_path, file)
                table_name = os.path.basename(file_path).split(".")[0]

                logging.info(f"Processing {table_name}")

                # create table
                create_table_from_csv(cursor, table_name=table_name, file_path=file_path)

                # upload data to stage
                upload_to_stage(cursor, file_path=file_path)

                # copy staged data into table
                copy_csv_into_table(cursor, table_name=table_name, file_path=file_path)

                logging.info(f"Successfully loaded {table_name} table.")
            connection.commit()
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
    finally:
        cursor.close()
        connection.close()
