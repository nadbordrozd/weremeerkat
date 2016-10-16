import os
import logging
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())
project_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)

logger = logging.getLogger(__name__)
logger.info('================START=================')
