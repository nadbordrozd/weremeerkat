import os
import pandas as pd
import numpy as np
from ml_metrics import mapk

from utils import logger

this_dir = os.getcwd()

train = pd.read_csv(os.path.join(this_dir, "../../data/raw/clicks_train.csv"))
test = pd.read_csv(os.path.join(this_dir, "../../data/raw/clicks_test.csv"))
sample_sub = pd.read_csv(os.path.join(this_dir, "../../data/raw/sample_submission.csv"))


def sort_ads(df, predictions):
    df['prediction'] = predictions
    disps = []
    ads = []
    current_group = []
    current_id = None
    for tup in df.itertuples():
        display_id = tup.display_id
        ad_id = tup.ad_id
        pred = tup.prediction
        if display_id != current_id:
            if current_id is not None:
                ads.append([ad_id for _, ad_id in sorted(current_group)])
            current_id = display_id
            disps.append(current_id)
            current_group = []

        current_group.append((-pred, ad_id))
    ads.append([ad_id for _, ad_id in sorted(current_group)])
    return pd.DataFrame({'display_id':disps, 'ad_id': ads}).sort_values('display_id')


def make_submission(model, output_path):
    logger.info('making final prediction with model %s' % model)
    predictions = model.predict(test)
    logger.info('done predicting, now sorting ads')
    df = sort_ads(test, predictions)
    df['ad_id'] = df.ad_id.map(lambda x: ' '.join(map(str, x)))
    logger.info('done sorting now saving result to %s' % output_path)
    df.to_csv(output_path)


def benchmark(model):
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
