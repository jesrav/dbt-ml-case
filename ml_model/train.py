import snowflake.connector
import pandas as pd

from config import config


def get_connection():
    return snowflake.connector.connect(
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PSW,
        account=config.SNOWFLAKE_ACCOUNT,
    )


def read_features(connection):
    SQL = f"""select top 100 * from analytics.{config.SNOWFLAKE_SCHEMA}.features"""
    return pd.read_sql(SQL, connection)


cnx = get_connection()

print(read_features(cnx).head())