from pathlib import Path

import pytest

from dagger.utils.importer import (
    _resolve_script_path,
    extract_module_functions,
)


TEST_SCRIPT_LOC = 'tests.fixtures.transform_script'
TEST_SCRIPT_FUNCS = {'var0', 'var1', 'var2', 'var3'}


@pytest.mark.parametrize('path', [
    Path('path/to/script.py'),
    'path.to.script',
])
def test_resolve_script_path(path):
    resolved = _resolve_script_path(path, check=False)
    assert resolved == Path('path/to/script.py')


def test_extract_functions():
    result = extract_module_functions(TEST_SCRIPT_LOC)
    assert set(result) == TEST_SCRIPT_FUNCS
