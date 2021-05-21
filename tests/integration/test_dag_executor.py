import pytest


from dagger.executor import DagExecutor



def test_executor():
    executor = DagExecutor('tests.fixtures.transform_script')
    data = {'a': 1, 'b': 2, 'c': 3}

    executor.plan()
    result = executor.apply(data)
    assert result == {
        'a': 1,
        'b': 2,
        'c': 3,
        'var1': 3,
        'var2': 9,
        'var3': 8,
        'var0': 9,
    }