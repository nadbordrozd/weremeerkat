import os
import logging
from dotenv import find_dotenv, load_dotenv
from joblib import Memory

load_dotenv(find_dotenv())
project_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), os.pardir))
raw_data_dir = os.path.join(project_dir, 'data/raw/')
interim_data_dir = os.path.join(project_dir, 'data/interim/')
ffm_trainsets_dir = os.path.join(project_dir, 'data/interim/features')
ffm_models_dir = os.path.join(project_dir, 'models/ffm')

full_train_csv_path = os.path.join(raw_data_dir, 'clicks_train.csv')
my_train_csv_path = os.path.join(interim_data_dir, 'my_clicks_train.csv')
cv_csv_path = os.path.join(interim_data_dir, 'clicks_cv.csv')

clicks_full_train_parquet = os.path.join(interim_data_dir, 'clicks_train.parquet')
clicks_my_train_parquet = os.path.join(interim_data_dir, 'clicks_my_train.parquet')
clicks_cv_parquet = os.path.join(interim_data_dir, 'clicks_cv.parquet')
clicks_test_parquet = os.path.join(interim_data_dir, 'clicks_test.parquet')

#MEMOIZATION
CACHE_DIR = os.path.join(project_dir, 'data/interim/cache')
cache = Memory(cachedir=CACHE_DIR).cache


def get_func_name(func):
    try:
        return func.func_name
    except AttributeError:
        return func.func.func_name


log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)


def get_logger():
    return logging.getLogger(__name__)


