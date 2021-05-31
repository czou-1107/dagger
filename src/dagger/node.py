import logging
from dataclasses import dataclass, field
from inspect import getsource, signature, Signature
from typing import Callable, List, Optional

from .utils.type_checker import check_type


logger = logging.getLogger(__name__)


@dataclass
class VariableNode:
    name: str
    dtype: type
    body: Optional[Callable] = None
    is_complete: bool = field(init=False)

    def __post_init__(self):
        if self.dtype is Signature.empty:
            self.dtype = None
        self.is_complete = self.body is not None


    def check_for_update(self, other):
        """ Provide logic for updating node """
        if self.name != other.name:
            return
        if self.dtype and other.dtype and self.dtype != other.dtype:
            raise ValueError(f'Node {self.name} has inconsistent dtypes: '
                             f'{self.dtype}, {other.dtype}')
        if not other.is_complete:
            return
        if self.is_complete:
            raise ValueError(f'Attempting to add duplicate node: {self.name}')
        self._update_body(other.body)


    def _update_body(self, body: Callable):
        self.body = body
        self.is_complete = True


    def __call__(self, *args, **kwargs):
        if not self.is_complete:
            raise ValueError('Can\'t call incomplete node')
        result = self.body(*args, **kwargs)
        check_type(result, self.dtype)
        return result


@dataclass
class FunctionSignatureTuple:
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
