from inspect import signature

import pytest

from dagger.node import FunctionSignatureTuple, VariableNode

from tests.fixtures.functions import untyped_func, typed_func


def node_from_func(func):
    """ This is really just used for testing """
    sig = signature(func)
    return VariableNode(name=func.__name__, body=func, dtype=sig.return_annotation)


@pytest.fixture
def untyped_node(untyped_func):
    yield node_from_func(untyped_func)

@pytest.fixture
def typed_node(typed_func):
    yield node_from_func(typed_func)


class TestVariableNode:
    @pytest.mark.parametrize('body,is_complete', [
        (lambda x: x, True), (None, False)
    ])
    def test_post_init_computes_is_complete(self, body, is_complete):
        node = VariableNode(name='func', body=body)
        assert node.is_complete == is_complete

    def test_post_init_converts_dtype_from_empty(self):
        def func(x):
            return x

        sig = signature(func)
        node = VariableNode(name='func', dtype=sig.return_annotation)
        assert node.dtype is None

    def test_check_for_update_idempotence(self, typed_node, untyped_node):
        assert not typed_node.check_for_update(typed_node)
        assert not untyped_node.check_for_update(untyped_node)

    def test_check_for_update_with_name_mismatch(self, untyped_node):
        other_node = VariableNode(name='other-name')
        assert not untyped_node.check_for_update(other_node)

    def test_check_for_update_with_incomplete(self, untyped_node):
        other_node = VariableNode(name=untyped_node.name, body=None)
        assert not untyped_node.check_for_update(other_node)

    def test_check_for_update_adds_dtype(self, untyped_node):
        other_node = VariableNode(name=untyped_node.name, dtype=int)
        assert not untyped_node.check_for_update(other_node)
        assert untyped_node.dtype is int

    def test_check_for_update_raises_on_inconsistent_dtype(self, typed_node):
        other_node = VariableNode(name=typed_node.name, dtype=float)
        print(typed_node.dtype, other_node.dtype)
        with pytest.raises(ValueError):
            typed_node.check_for_update(other_node)

    def test_check_for_update_raises_on_completed_node(self, typed_node, untyped_node):
        with pytest.raises(ValueError):
            untyped_node.check_for_update(typed_node)
        with pytest.raises(ValueError):
            typed_node.check_for_update(untyped_node)

    def test_check_for_update_success(self, typed_node):
        """ This checks _update_body() as well """
        incomplete_node = VariableNode(name='func')
        assert incomplete_node.check_for_update(typed_node)
        assert incomplete_node.is_complete
        assert incomplete_node.body is typed_node.body

    def test_call(self, untyped_node, untyped_func):
        assert untyped_node(a=1, b=2) == untyped_func(a=1, b=2)


class TestFunctionSignatureTuple:
    def test_from_signature(self, typed_func, typed_node):
        tup = FunctionSignatureTuple.from_signature(typed_func)
        assert tup.node == typed_node
        assert tup.parents == [VariableNode('a', int), VariableNode('b', int)]