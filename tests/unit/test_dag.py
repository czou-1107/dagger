import pytest

from dagger.dag import DagExecutor
from dagger.node import VariableNode
from dagger.utils.importer import extract_module_functions

from tests.fixtures.functions import typed_func, untyped_func


@pytest.fixture(scope='module')
def mock_nodes():
    a = VariableNode(name='a', body=None)
    b = VariableNode(name='b', body=None)
    c = VariableNode(name='c', body=sum)

    yield a, b, c

@pytest.fixture(scope='module')
def mock_script_loc():
    yield 'tests.fixtures.transform_script'


@pytest.fixture(scope='module')
def mock_funcs(mock_script_loc):
    yield extract_module_functions(mock_script_loc)


class TestInitializationFunctions:
    def test__add_or_update_node_new_node(self, mock_nodes):
        ex = DagExecutor()
        ex._add_or_update_node(mock_nodes[0])
        assert ex['a'] == mock_nodes[0]

    def test__add_or_update_node_existing_node(self, mock_nodes):
        ex = DagExecutor()
        ex._add_or_update_node(mock_nodes[0])
        # Add body to node:
        mock_nodes[0].body = sum
        ex._add_or_update_node(mock_nodes[0])
        assert ex['a'] == mock_nodes[0]

    def test__add_function_nodes(self, typed_func):
        ex = DagExecutor()
        ex._add_function_nodes(typed_func)
        for node in ['a', 'b', 'func']:
            assert ex[node]
        assert ('a', 'func') in ex._graph.edges and ('b', 'func') in ex._graph.edges


class TestAddFunctions:
    def test_add_funcs(self, mock_funcs):
        ex = DagExecutor()
        ex.add_functions(mock_funcs)
        for fname in mock_funcs:
            assert fname in ex

    def test_break_on_cycle(self):
        def a1(a2):
            return

        def a2(a1):
            return

        ex = DagExecutor()
        with pytest.raises(ValueError):
            ex.add_functions({'a1': a1, 'a2': a2})

    def test_cant_add_if_already_planned(self, mock_funcs):
        ex = DagExecutor()
        ex.plan()
        with pytest.raises(ValueError):
            ex.add_functions(mock_funcs)


def test_add_function_scripts(mock_script_loc, mock_funcs):
    ex = DagExecutor()
    ex.add_function_scripts(mock_script_loc)
    for fname in mock_funcs:
        assert fname in ex


class TestPlan:
    def test_with_mock_script(self, mock_script_loc):
        ex = DagExecutor()
        ex.add_function_scripts(mock_script_loc)
        ex.plan()

        assert ex.initial_nodes == {'a', 'b', 'c'}
        # This is actually not a terrific test since the topological sort is not
        # guaranteed to be stable!
        assert [node.name for node in ex.execution_plan] == ['var1', 'var2', 'var3', 'var0']


class TestApply:
    # This is really in the realm of integration test
    pass
