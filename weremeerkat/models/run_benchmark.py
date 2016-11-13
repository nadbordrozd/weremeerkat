# -*- coding: utf-8 -*-
import click
import pandas as pd
import numpy as np
from benchmarks import sort_ads, evaluate_ranking, \
    evaluate_predictions, make_submission_file, read_predictions
import json

from weremeerkat.utils import get_logger
import weremeerkat.paths as paths


@click.command()
@click.argument('feature_set')
def main(feature_set):
    """'will look for train and test in data/interim/features/feature_set'"""
    logger = get_logger()
    logger.info('evaluating and creating submission file for %s feature set' % feature_set)
    cv_predictions_path = paths.get_cv_predictions_path(feature_set)
    test_predictions_path = paths.get_test_predictions_path(feature_set)

    logger.info('reading predictions for cross validation set')
    cv_with_prediction = read_predictions(cv_predictions_path, paths.cv_csv_path)
    roc_auc = evaluate_predictions(cv_with_prediction)
    logger.info('roc auc = %s' % roc_auc)
    logger.info('now sorting ads')
    cv_sorted_ads = sort_ads(cv_with_prediction)
    logger.info('making cv submission')
    map12 = evaluate_ranking(cv_with_prediction, cv_sorted_ads)
    make_submission_file(cv_sorted_ads, paths.get_cv_submission_path(feature_set))
    logger.info('mean average precision at 12 for %s = %s' % (feature_set, map12))

    with open(paths.get_metrics_path(feature_set), 'wb') as f:
        json.dump({'roc_auc': roc_auc, 'map12': map12}, fp=f)

    logger.info('reading test predictions')
    test_with_prediction = read_predictions(test_predictions_path, paths.test_csv_path)
    test_sorted_ads = sort_ads(test_with_prediction)
    logger.info('making test submission file')
    make_submission_file(test_sorted_ads, paths.get_submission_path(feature_set))
    logger.info('done and done')

if __name__ == '__main__':
    main()
