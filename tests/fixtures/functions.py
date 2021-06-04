"""
Mock functions as fixtures
"""
import pytest


@pytest.fixture
def untyped_func():
    def func(a, b):
        return a + b
    yield func

@pytest.fixture
def typed_func():
    def func(a: int, b: int) -> int:
        return a + b
    yield func