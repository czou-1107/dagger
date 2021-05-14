import pytest

from dagger.generator.importer import (
    extract_functions,
)

from dagger.generator.inspector import (
    TransformNode,
)


TEST_SCRIPT_LOC = 'tests.fixtures.transform_script'

def test_extract_functions():
    result = extract_functions(TEST_SCRIPT_LOC)
    assert set(result) == {'var1', 'var2'}


@pytest.fixture()
def extracted_functions():
    yield extract_functions(TEST_SCRIPT_LOC)


def test_transform_node(extracted_functions):
    for _, f in extracted_functions.items():
        print(f)
        node = TransformNode.from_callable(f)
        print(node)