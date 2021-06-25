"""
Runtime script for Boston data.

Utilizes the transform script: boston_housing_transformations.py
"""
from pathlib import Path

import pandas as pd
from sklearn.datasets import load_boston

from dagger.dag import DagExecutor


SCRIPT_LOC = Path(__file__).parent / 'boston_housing_transformations.py'

print('** DEMO SCRIPT **')

print('Loading in Boston data...')
boston_data_bunch = load_boston(return_X_y=False)
boston_data = pd.DataFrame(boston_data_bunch.data, columns=boston_data_bunch.feature_names)
boston_data['MEDV'] = boston_data_bunch.target
print('Boston data:', boston_data.head(5))

print(f'Plan execution dag defined in {SCRIPT_LOC}...')
executor = DagExecutor().add_function_scripts(SCRIPT_LOC)
executor.plan()
print('Applying dag to data...')
executor.apply(boston_data)
print('Done! New boston data:', boston_data.head(5))
