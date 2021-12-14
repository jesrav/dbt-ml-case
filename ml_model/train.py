from pprint import pprint

import snowflake.connector
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

from model_config import ml_pipeline, get_feature_importances
from config import config


TARGET_COL = "RAINTOMORROW"
TARGET_COL_ENCODED = "RAINTOMORROW_ENC"


def get_connection():
    return snowflake.connector.connect(
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PSW,
        account=config.SNOWFLAKE_ACCOUNT,
    )


def read_features(connection):
    SQL = f"""select top 10000 * from analytics.{config.SNOWFLAKE_SCHEMA}.features"""
    return pd.read_sql(SQL, connection)


def create_encoded_target(df, col):
    return df[col].replace({"Yes": 1, "No": 0})


def drop_missing_targets(df, target_col):
    return df.dropna(subset=[target_col])


cnx = get_connection()

df = read_features(cnx)

df = drop_missing_targets(df, target_col=TARGET_COL)

df[TARGET_COL_ENCODED] = create_encoded_target(df, col=TARGET_COL)

train, test = train_test_split(df)
ml_pipeline.fit(train, train[TARGET_COL_ENCODED])

predictions = ml_pipeline.predict(test)

print(classification_report(test[TARGET_COL_ENCODED], predictions))

pprint(get_feature_importances(ml_pipeline))