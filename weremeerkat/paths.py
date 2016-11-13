import os

from weremeerkat.utils import project_dir, ffm_models_dir, \
    ffm_trainsets_dir, raw_data_dir, interim_data_dir

full_train_csv_path = os.path.join(raw_data_dir, 'clicks_train.csv')
my_train_csv_path = os.path.join(interim_data_dir, 'my_clicks_train.csv')
cv_csv_path = os.path.join(interim_data_dir, 'clicks_cv.csv')
test_csv_path = os.path.join(raw_data_dir, 'clicks_test.csv')

def get_model_dir(feature_set):
    return os.path.join(ffm_models_dir, feature_set)


def get_cv_model_path(feature_set):
    model_dir = get_model_dir(feature_set)
    return os.path.join(model_dir, 'cv.model')


def get_full_model_path(feature_set):
    model_dir = get_model_dir(feature_set)
    return os.path.join(model_dir, 'full.model')


def get_predictions_dir(feature_set):
    return os.path.join(project_dir, 'data', 'processed', feature_set)


def get_cv_predictions_path(feature_set):
    predictions_dir = get_predictions_dir(feature_set)
    return os.path.join(predictions_dir, 'cv_predictions.txt')


def get_test_predictions_path(feature_set):
    predictions_dir = get_predictions_dir(feature_set)
    return os.path.join(predictions_dir, 'tett_predictions.txt')


def get_ffm_train_dir(feature_set):
    return os.path.join(ffm_trainsets_dir, feature_set)


def get_ffm_cv_path(feature_set):
    return os.path.join(get_ffm_train_dir(feature_set), 'cv', 'part-00000')


def get_ffm_my_train_path(feature_set):
    return os.path.join(get_ffm_train_dir(feature_set), 'train', 'part-00000')


def get_ffm_test_path(feature_set):
    return os.path.join(get_ffm_train_dir(feature_set), 'test', 'part-00000')


def get_ffm_full_train_path(feature_set):
    return os.path.join(get_ffm_train_dir(feature_set), 'full_train', 'part-00000')


def get_metrics_path(feature_set):
    return os.path.join(get_predictions_dir(feature_set), 'metrics.json')


def get_submission_path(feature_set):
    return os.path.join(get_predictions_dir(feature_set), 'sub.csv')


def get_cv_submission_path(feature_set):
    return os.path.join(get_predictions_dir(feature_set), 'cv_sub.csv')
