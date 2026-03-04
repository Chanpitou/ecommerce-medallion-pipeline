import snowflake.connector
import os
from src.utils.config import SNOWFLAKE_CONFIG


def get_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
    )


def upload_to_stage(cursor, file_path):
    cursor.execute(
        f"PUT file://{os.path.abspath(file_path)} @internal_stage AUTO_COMPRESS=TRUE"
        # Compressing file into .gz when in stage
    )


def copy_csv_into_table(cursor, table_name, file_name):
    cursor.execute(
        f"""
        COPY INTO {table_name}
        FROM @internal_stage/{file_name}.gz
        FILE_FORMAT = (FORMAT_NAME = bronze.csv_format)
        ON_ERROR = 'CONTINUE';
    """
    )


def copy_json_into_table(cursor, table_name, file_name):
    cursor.execute(
        f"""
        COPY INTO {table_name}(raw_data)
        FROM @internal_stage/{file_name}.gz
        FILE_FORMAT = (FORMAT_NAME = bronze.json_format)
        ON_ERROR = 'CONTINUE';
    """
    )
