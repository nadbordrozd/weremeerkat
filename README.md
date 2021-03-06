top secret
==========
###Setup
Create `.env` file in the project root folder. It needs to look like this:

```text
KAGGLE_USER='my kaggle login'
KAGGLE_PASSWORD='my kaggle password'
```

Put this directory on your python path like this:
```bash
export PYTHONPATH="${PYTHONPATH}:PATH_TO_THIS_DIRECTORY"
```

Put it in your `bash_profile` (or equivalent) to make it permanent.

then run 
```bash
weremeerkat/data/download_data.sh
```

To get the good stuff. Code for testing the models and making submissions lives in src/models/models and src/models/benchmarks at the moment. The idea is to define all the reusable components there and to do one-off experiments using those components in src/models/run.py.

```bash
python weremeerkat/models/run.py
```

To put the csv filer in a sqlite database run:

```bash
weremeerkat/data/create_database.sh
```

This will create `data/interim/database.db` and import all the tables into it.
To instead save everything as spark friendly parquet files do:

```bash
spark-submit weremeerkat/data/make_parquet.py
```

You must have spark installed and available on path. Then make train/test split by running
```bash
spark-submit --driver-memory 25g weremeerkat/data/train_test_split.py
```

Next to create training set for libffm, run:
```bash
spark-submit --driver-memory 25g weremeerkat/data/features/build_features.py
```
This will create a bunch of files in `data/interim/features/basic` - full train set, test set, and train set split 80-20 for cross validation. The idea is that each new set of features `new_features` will create files in `data/interim/features/new_features` and the downstream processing can be the same.

Next run libffm:
```bash
python weremeerkat/models/run_benchmark.py basic
```

This will train the model and save it in `models/basic` and make predictions in `data/processed/basic`. The `basic` argument refers to the training set created with the previous command. Next, evaluate the results and make submission file by running:
 
```bash
python weremeerkat/models/run_benchmark.py basic
```

Submission file and a file with evaluation metrics will appear in `data/processed/basic`
 

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.testrun.org


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
