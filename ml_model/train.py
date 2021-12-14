import snowflake.connector
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import FeatureUnion, Pipeline
from sklearn.impute import SimpleImputer

from config import config


def get_connection():
    return snowflake.connector.connect(
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PSW,
        account=config.SNOWFLAKE_ACCOUNT,
    )


def read_features(connection):
    SQL = f"""select top 10000 * from analytics.{config.SNOWFLAKE_SCHEMA}.features"""
    return pd.read_sql(SQL, connection)


def drop_missing_targets(df, target_col):
    return df.dropna(subset=[target_col])


cnx = get_connection()

df = read_features(cnx)

target_col = "RAINTOMORROW"
feature_cols = [col for col in df.columns if col not in [target_col, "DATE", "CREATED_AT"]]
categirocal_features = ['LOCATION','WINDGUSTDIR', 'WINDDIR3PM', 'RAINTODAY']
numerical_features = [col for col in feature_cols if col not in categirocal_features]

df = drop_missing_targets(df, target_col)

target = df[target_col]
features = df[feature_cols]

categorical_feature_pipeline = Pipeline([
        ("column_selector", ColumnTransformer(transformers=[('selector', 'passthrough', categirocal_features)], remainder="drop")),
        ('encoding', OneHotEncoder()),
        ('imputation', SimpleImputer(missing_values=np.nan, strategy='most_frequent'))
])

numerical_feature_pipeline = Pipeline([
        ("column_selector", ColumnTransformer(transformers=[('selector', 'passthrough', numerical_features)], remainder="drop")),
        ('imputation', SimpleImputer(missing_values=np.nan, strategy='mean'))
])


feature_union = FeatureUnion([
    ("categorical", categorical_feature_pipeline),
    ("non_categorical", numerical_feature_pipeline)
])

ml_pipeline = Pipeline([
    ("feature_preprocess", feature_union),
    ("classifier", RandomForestClassifier())
])

ml_pipeline.fit(features, target)