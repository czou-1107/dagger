import pytest
from pandas.testing import assert_frame_equal

from dagger.__main__ import plan_apply_dag

from samples import boston_housing_run
from tests.fixtures.transform_script import TEST_DATA, EXPECTED_OUTPUT


@pytest.mark.parametrize('use_dask', [True, False])
def test_execute_transform_script(use_dask):
    result = plan_apply_dag('tests.fixtures.transform_script', TEST_DATA, use_dask=use_dask)
    # Frames can be in incorrect order:
    assert set(result) == set(EXPECTED_OUTPUT)
    assert_frame_equal(result[EXPECTED_OUTPUT.columns], EXPECTED_OUTPUT)


def test_sample_scripts_boston():
    result = boston_housing_run.main()
    assert result.shape == (506, 20)  # Very simple ad hoc check. Could be better
