# This script is to be read in as text and processed!
# It is paired with a data fixture: data.py
import numpy as np
import pandas as pd


def var0(var1, var2):
    return np.maximum(var1, var2)


def var1(a: float, b: float):
    return a + b


def var2(var1, c: float):
    return var1 * c


def var3(var2, a: float):
    return var2 - a


THIS_IS_NOT_A_FUNCTION = 1


class ThisIsNotAFunction:
    pass


TEST_DATA = pd.DataFrame([{'a': 1, 'b': 2, 'c': 3}])
EXPECTED_OUTPUT = pd.DataFrame([
    {
        'var0': 9, 'var1': 3, 'var2': 9, 'var3': 8,
        **TEST_DATA.to_dict(orient='records')[0],
    }
])