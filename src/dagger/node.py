import logging
from dataclasses import dataclass, field
from inspect import signature, Signature
from typing import Callable, List, Optional

from .utils.type_checker import check_type


logger = logging.getLogger(__name__)


@dataclass
class VariableNode:
    """ Representation of a variable in a DAG

    Contains variable name, type, and transformation function defining variable
    """
    name: str
    dtype: Optional[type] = None
    body: Optional[Callable] = None
    is_complete: bool = field(init=False)

    def __post_init__(self):
        if self.dtype is Signature.empty:
            self.dtype = None
        self.is_complete = self.body is not None


    def check_for_update(self, other) -> bool:
        """ Provide logic for updating node

        Returns whether body was updated (and node converts to complete state) """
        if self.name != other.name or self == other:
            return False
        if self.dtype and other.dtype and self.dtype != other.dtype:
            raise ValueError(f'Node {self.name} has inconsistent dtypes: '
                             f'{self.dtype}, {other.dtype}')
        if not self.dtype and other.dtype:
            self.dtype = other.dtype  # Assign it if no conflict
        if not other.is_complete:
            return False
        if self.is_complete:
            raise ValueError(f'Attempting to add duplicate node: {self.name}')
        self._update_body(other.body)
        return True


    def _update_body(self, body: Callable):
        self.body = body
        self.is_complete = True


    def __call__(self, *args, **kwargs):
        if not self.is_complete:
            raise ValueError('Can\'t call incomplete node')
        result = self.body(*args, **kwargs)
        check_type(result, self.dtype)
        return result


    def _keys(self):
        return (self.name, self.dtype, self.body)


    def __eq__(self, other):
        if not isinstance(other, VariableNode):
            return False
        return self._keys() == other._keys()


@dataclass
class FunctionSignatureTuple:
    """ Representation of variable nodes generated by a single function

    The function output is a VariableNode, and its inputs are a list of VariableNode(s)
    """
    node: VariableNode
    parents: List[VariableNode]


    @classmethod
    def from_signature(cls, func):
        sig = signature(func)
        node = VariableNode(name=func.__name__,
                            dtype=sig.return_annotation,
                            body=func)
        parents = [
            VariableNode(name=pnm, dtype=param.annotation, body=None)
            for pnm, param in sig.parameters.items()
        ]

        return cls(node=node, parents=parents)
