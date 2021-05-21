import pytest

from dagger.utils.importer import (
    _resolve_script_path,
    _split_path,
    _get_module_funcs,
    extract_module_functions,
)


TEST_SCRIPT_LOC = 'tests.fixtures.transform_script'


@pytest.mark.skip
def test_resolve_script_path():
    assert False


@pytest.mark.skip
def test_split_path():
    assert False


@pytest.mark.skip
def test_get_module_funcs():
    assert False


def test_extract_functions():
    result = extract_module_functions(TEST_SCRIPT_LOC)
    assert set(result) == {'var0', 'var1', 'var2', 'var3'}
