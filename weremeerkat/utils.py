import os
import logging
from dotenv import find_dotenv, load_dotenv
from joblib import Memory

load_dotenv(find_dotenv())
project_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), os.pardir))
raw_data_dir = os.path.join(project_dir, 'data/raw/')
interim_data_dir = os.path.join(project_dir, 'data/interim/')

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

logger = logging.getLogger(__name__)
logger.info('================START=================')
