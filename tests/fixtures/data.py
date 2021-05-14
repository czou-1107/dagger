import numpy as np
import pandas as pd
import pytest

DATA_SIZE = 100


@pytest.fixture()
def mock_data():
    yield pd.DataFrame({
        'a': np.random.exponential(size=DATA_SIZE),
        'b': np.random.normal(size=DATA_SIZE),
        'c': np.random.uniform(low=0, high=100, size=DATA_SIZE),
    })
