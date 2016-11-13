# -*- coding: utf-8 -*-
import click
import subprocess

from weremeerkat.utils import get_logger
import weremeerkat.paths as paths


@click.command()
@click.argument('feature_set')
def main(feature_set):
    """'will look for train and test in data/interim/features/feature_set'"""
    logger = get_logger()
    logger.info('running ffm with %s' % feature_set)

    train_path = paths.get_ffm_my_train_path(feature_set)
    full_train_path = paths.get_ffm_full_train_path(feature_set)
    test_path = paths.get_ffm_test_path(feature_set)
    cv_path = paths.get_ffm_cv_path(feature_set)

    model_dir = paths.get_model_dir(feature_set)
    cv_model_path = paths.get_cv_model_path(feature_set)
    full_model_path = paths.get_full_model_path(feature_set)

    predictions_dir = paths.get_predictions_dir(feature_set)
    cv_predictions_path = paths.get_cv_predictions_path(feature_set)
    test_predictions_path = paths.get_test_predictions_path(feature_set)

    subprocess.call('mkdir -p %s' % predictions_dir, shell=True)
    subprocess.call('mkdir -p %s' % model_dir, shell=True)

    command = 'ffm-train -p %s --auto-stop  %s %s' % (cv_path, train_path, cv_model_path)
    logger.info('now training on my training set (not full training set')
    subprocess.call(command, shell=True)

    logger.info('now making predictions on cv set')
    subprocess.call('ffm-predict %s %s %s' % (cv_path, cv_model_path, cv_predictions_path),
                    shell=True)

    logger.info('now training full training set')
    command = 'ffm-train -t 1 %s %s' % (full_train_path, full_model_path)
    subprocess.call(command, shell=True)

    logger.info('now making predictions on test set')
    subprocess.call('ffm-predict %s %s %s' % (test_path, full_model_path, test_predictions_path),
                    shell=True)

    logger.info('done and done')

if __name__ == '__main__':
    main()
