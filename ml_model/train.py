import json 
import pickle

import snowflake.connector
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import numpy as np
from sklearn2pmml import sklearn2pmml

from model_config import ml_pipeline, get_feature_importances
from config import config

pd.set_option("display.max_rows", None, "display.max_columns", None)

CLASSIFICATION_REPORT_PATH = "artifacts/classification_report.json"
FEATURE_IMPORTANCE_PATH = "artifacts/feature_importances.csv"
MODEL_PATH = "artifacts/model.pickle"

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


def convert_non_to_nan(df):
    return df.replace([None], np.nan, inplace=False)


def create_encoded_target(df, col):
    return df[col].replace({"Yes": 1, "No": 0})


def drop_missing_targets(df, target_col):
    return df.dropna(subset=[target_col])


def main():
    cnx = get_connection()

    print("Read and preprocess modelling data.")
    df = read_features(cnx)

    df = convert_non_to_nan(df)

    df = drop_missing_targets(df, target_col=TARGET_COL)

    df[TARGET_COL_ENCODED] = create_encoded_target(df, col=TARGET_COL)

    print("Train model.")
    train, test = train_test_split(df)
    ml_pipeline.fit(train, train[TARGET_COL_ENCODED])

    print("Evaluate model on hold out set and save artifacts.")
    predictions = ml_pipeline.predict(test)

    with open(CLASSIFICATION_REPORT_PATH, "w") as f:
        json.dump(
            classification_report(test[TARGET_COL_ENCODED], predictions, output_dict=True), f
        )
    
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(ml_pipeline, f)
    
    get_feature_importances(ml_pipeline).to_csv(FEATURE_IMPORTANCE_PATH)

    sklearn2pmml(ml_pipeline, "artifacts/model.pmml", with_repr = True)

if __name__ == "__main__":
    main()


