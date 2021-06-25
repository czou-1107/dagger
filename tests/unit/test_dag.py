import pytest

from dagger.dag import DagExecutor
from dagger.node import VariableNode


TEST_SCRIPT_LOC = 'tests.fixtures.transform_script'


@pytest.fixture(scope='module')
def mock_nodes():
    a = VariableNode(name='a', body=None)
    b = VariableNode(name='b', body=None)
    c = VariableNode(name='c', body=sum)

    yield a, b, c


# This module is a single class
class TestInitializationFunctions:
    def test_add_or_update_node_new_node(self, mock_nodes):
        ex = DagExecutor()
        ex._add_or_update_node(mock_nodes[0])
        assert ex['a'] == mock_nodes[0]

