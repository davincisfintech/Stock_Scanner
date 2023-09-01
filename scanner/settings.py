import logging
import os
import warnings
from datetime import datetime
from pathlib import Path

import pytz

warnings.filterwarnings('ignore')

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / 'config'
LOGS_DIR = BASE_DIR / 'logs'
RECORDS_DIR = BASE_DIR / 'records'
DATA_DIR = BASE_DIR / 'data'
TZ = pytz.timezone('US/Eastern')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(message)s')

if not os.path.exists(LOGS_DIR):
    os.mkdir(LOGS_DIR)
file_handler = logging.FileHandler(BASE_DIR / f'logs/{datetime.now(tz=TZ).date()}_run.log')

file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def customTime(*args):
    utc_dt = pytz.utc.localize(datetime.utcnow())
    converted = utc_dt.astimezone(TZ)
    return converted.timetuple()


logging.Formatter.converter = customTime
