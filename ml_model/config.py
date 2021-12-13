import os

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


class Config:
    ENVIRONMENT = "dev"
    SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
    SNOWFLAKE_PSW = os.environ["SNOWFLAKE_PSW"]
    SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]


class DevConfig(Config):
    SNOWFLAKE_SCHEMA = os.environ["SNOWFLAKE_DEV_SCHEMA"]


class ProdConfig(Config):
    ENVIRONMENT = "prod"
    SNOWFLAKE_SCHEMA = "public"


config = ProdConfig if os.environ["ENVIRONMENT"] == 'prod' else DevConfig
