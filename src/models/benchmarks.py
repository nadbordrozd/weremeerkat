import os
import pandas as pd
from itertools import izip
import numpy as np
from ml_metrics import mapk

from utils import logger

this_dir = os.getcwd()

train = pd.read_csv(os.path.join(this_dir, "../../data/raw/clicks_train.csv"))
test = pd.read_csv(os.path.join(this_dir, "../../data/raw/clicks_test.csv"))
sample_sub = pd.read_csv(os.path.join(this_dir, "../../data/raw/sample_submission.csv"))

def sort_ads(df, predictions):
    """groups ad_ids per display_id, sorts them by predicted click probability.
    returns dataframe

    given this:

    df = pd.DataFrame({
        'display_id': [1,1,1,2,2,2,3,3],
        'ad_id':      [1,2,3,4,5,6,7,8]})

    and this:
    predictions =     [2,3,4,1,5,2,2,7]

    returns this:
    pd.DataFrame({
        'display_id': [1,2,3]
        'ad_id': [[3,2,1],[5,6,4],[8,7]]
    })


    it's rather inelegant but works much faster than using dataframe's builtin groupby
    NOTE: input dataframe must be sorted by display_id"""
    disps = []
    ads = []
    current_group = []
    current_id = None
    for tup, pred in izip(df.itertuples(), predictions):
        display_id = tup.display_id
        ad_id = tup.ad_id

        if display_id != current_id:
            if current_id is not None:
                ads.append([x for _, x in sorted(current_group)])
                disps.append(current_id)
            current_id = display_id
            current_group = []
        current_group.append((-pred, ad_id))

    ads.append([ad_id for _, ad_id in sorted(current_group)])
    disps.append(current_id)
    return pd.DataFrame({'display_id': disps, 'ad_id': ads})


def make_submission(model, filename):
    logger.info('making final prediction with model %s' % model)
    predictions = model.predict(test)
    logger.info('done predicting, now sorting ads')
    df = sort_ads(test, predictions)
    df['ad_id'] = df.ad_id.map(lambda x: ' '.join(map(str, x)))
    output_path = os.path.join(this_dir, '../../data/processed', filename)
    logger.info('done sorting now saving result to %s' % output_path)
    df.to_csv(output_path)


def benchmark(model):
    np.random.seed(0)
    logger.info('making train-test split')
    ids = train.display_id.unique()
    ids = np.random.choice(ids, size=len(ids)//10, replace=False)

    val_set = train[train.display_id.isin(ids)].reindex().sort_values('display_id')
    trainset = train[~train.display_id.isin(ids)].reindex()

    logger.info('training model %s on trainset' % model)
    model.fit(trainset, trainset.clicked)
    logger.info('done training')

    logger.info('making predictions on validation set')
    preds = model.predict(val_set)
    logger.info('sorting ads according to predicted probability')
    ads_sorted = sort_ads(val_set, preds)

    y = val_set[val_set.clicked==1].ad_id.values
    y = [[_] for _ in y]
    result = mapk(y, list(ads_sorted.ad_id), k=12)

    logger.info('%s mapk = %.3f' % (model, result))
    return result
