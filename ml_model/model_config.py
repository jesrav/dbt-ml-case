from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import FeatureUnion
from sklearn.impute import SimpleImputer
from sklearn2pmml.pipeline import PMMLPipeline
import numpy as np
import pandas as pd

FEATURE_COLS = [
    #'LOCATION', 
    'WINDGUSTDIR', 
    'WINDDIR3PM', 
    'RAINTODAY', 
    'MINTEMP', 
    'MAXTEMP', 
    'RAINFALL', 
    'EVAPORATION', 
    'SUNSHINE', 
    'PRESSURE9AM', 
    'PRESSURE3PM', 
    'TEMP9AM', 
    'TEMP3PM', 
    'WINDGUSTSPEED',
    'WINDSPEED9AM', 
    'WINDSPEED3PM', 
    'HUMIDITY9AM', 
    'HUMIDITY3PM', 
    'CLOUD9AM', 
    'CLOUD3PM', 
    # 'MINTEMP_YESTERDAY', 
    # 'MAXTEMP_YESTERDAY', 
    # 'RAINFALL_YESTERDAY', 
    # 'EVAPORATION_YESTERDAY', 
    # 'SUNSHINE_YESTERDAY', 
    # 'PRESSURE9AM_YESTERDAY', 
    # 'PRESSURE3PM_YESTERDAY', 
    # 'TEMP9AM_YESTERDAY', 
    # 'TEMP3PM_YESTERDAY', 
    # 'WINDGUSTSPEED_YESTERDAY', 
    # 'WINDSPEED9AM_YESTERDAY',
    # 'WINDSPEED3PM_YESTERDAY', 
    # 'HUMIDITY9AM_YESTERDAY', 
    # 'HUMIDITY3PM_YESTERDAY', 
    # 'CLOUD9AM_YESTERDAY', 
    # 'CLOUD3PM_YESTERDAY',
    'MINTEMP_5D_MA', 
    'MAXTEMP_5D_MA',
    'RAINFALL_5D_MA', 
    'EVAPORATION_5D_MA', 
    'SUNSHINE_5D_MA', 
    'PRESSURE9AM_5D_MA', 
    'PRESSURE3PM_5D_MA', 
    'TEMP9AM_5D_MA',
    'TEMP3PM_5D_MA', 
    'WINDGUSTSPEED_5D_MA', 
    'WINDSPEED9AM_5D_MA', 
    'WINDSPEED3PM_5D_MA', 
    'HUMIDITY9AM_5D_MA', 
    'HUMIDITY3PM_5D_MA', 
    'CLOUD9AM_5D_MA', 
    'CLOUD3PM_5D_MA'
]
CATEGORICAL_COLS = ['LOCATION','WINDGUSTDIR', 'WINDDIR3PM', 'RAINTODAY']
NUMERICAL_COLS = [col for col in FEATURE_COLS if col not in CATEGORICAL_COLS]

categorical_feature_pipeline = PMMLPipeline([
        ("column_selector", ColumnTransformer(transformers=[('selector', 'passthrough', CATEGORICAL_COLS)], remainder="drop")),
        ('imputation', SimpleImputer(missing_values=np.nan, strategy='most_frequent')),
        ('encoding', OneHotEncoder()),
])

numerical_feature_pipeline = PMMLPipeline([
        ("column_selector", ColumnTransformer(transformers=[('selector', 'passthrough', NUMERICAL_COLS)], remainder="drop")),
        ('imputation', SimpleImputer(missing_values=np.nan, strategy='mean'))
])


feature_union = FeatureUnion([
    ("categorical", categorical_feature_pipeline),
    ("non_categorical", numerical_feature_pipeline)
])

ml_pipeline = PMMLPipeline([
    ("feature_preprocess", feature_union),
    ("classifier", RandomForestClassifier())
])


def get_feature_names(ml_pipeline):
    ml_pipeline["feature_preprocess"].transformer_list[0][1][2].feature_names_in_ = CATEGORICAL_COLS
    return (
        ml_pipeline["feature_preprocess"].transformer_list[0][1][2].get_feature_names_out().tolist()
        + NUMERICAL_COLS
    )


def get_feature_importances(ml_pipeline):
    return pd.DataFrame(
        zip(get_feature_names(ml_pipeline), ml_pipeline["classifier"].feature_importances_),
        columns=["feature", "importance"]
    ).sort_values(by="importance", ascending=False)
    