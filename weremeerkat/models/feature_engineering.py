import os
import pandas as pd

from utils import cache

this_dir = os.getcwd()
TRAIN_PATH = os.path.join(this_dir, "../../data/raw/clicks_train.csv")
TEST_PATH = os.path.join(this_dir, "../../data/raw/clicks_test.csv")
SAMPLE_SUB_PATH = os.path.join(this_dir, "../../data/raw/sample_submission.csv")
EVENTS_PATH = os.path.join(this_dir, "../../data/raw/events.csv")

@cache
def get_train_set():
    return pd.read_csv(TRAIN_PATH)


@cache
def get_test_set():
    return pd.read_csv(TEST_PATH)


def get_total():
    """returns train+test sets combined and joined with events table"""
    train = pd.read_csv(TRAIN_PATH)
    test = pd.read_csv(TEST_PATH)
    total = train.append(test)
    events = pd.read_csv(EVENTS_PATH)
    return pd.merge(total, events, on='display_id')

def get_train_test(tot_df):
    """splits combined train + test dataframe back into 2"""
    # FIXME: ugly hardcode
    train_size = 16874593
    return (tot_df[tot_df.display_id <= train_size], tot_df[tot_df.display_id > train_size])
