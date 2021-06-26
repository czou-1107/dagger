import pytest

from dagger.__main__ import plan_apply_dag

from samples import boston_housing_run
from tests.fixtures.transform_script import TEST_DATA, EXPECTED_OUTPUT


def test_execute_transform_script():
    assert plan_apply_dag('tests.fixtures.transform_script', TEST_DATA) == EXPECTED_OUTPUT


def test_sample_scripts_boston():
    result = boston_housing_run.main()
    assert result.shape == (506, 20)  # Very simple ad hoc check. Could be better
