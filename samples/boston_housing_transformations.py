"""
Transform script for Boston data. These transformations are pretty much non-sensical!

Companion to the runtime script: boston_housing_run.py
"""
import numpy as np
import pandas as pd


def BLK_PROP(B: float) -> float:
    """ B comes transformed as 1000 * (Bk - 0.63) ^ 2. Recover this """
    return np.sqrt(B) / 1000 + 0.63


def BLK_DECILE(B: float) -> int:
    return pd.qcut(B, q=10, duplicates='drop', labels=False)


def LSTAT_BLK_PROP(BLK_PROP: float, LSTAT: float) -> float:
    return BLK_PROP * LSTAT


def LOG_AVG_TAX_VAL(MEDV: float, TAX: float) -> float:
    return np.log(MEDV * TAX)


def ZN_OR_INDUS(ZN: float, INDUS: float) -> float:
    return ZN + INDUS

def LAVG_TAX_ZN_OR_INDUS(LOG_AVG_TAX_VAL: float, ZN_OR_INDUS: float) -> float:
    return LOG_AVG_TAX_VAL * ZN_OR_INDUS
