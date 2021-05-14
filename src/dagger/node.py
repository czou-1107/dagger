from dataclasses import dataclass, field
from inspect import Signature
from typing import Callable, Dict, List, Optional, Union


@dataclass
class VariableNode:
    """
    Representation of a variable in a computation DAG.
    
    If instantiated with :body:, then it is a computed node
    """
    name: str
    dtype: type
    body: Optional[Callable] = None
    is_initial_node: bool = field(init=False)

    def __post_init__(self):
        if self.dtype is Signature.empty:
            self.dtype = None
        self.is_initial_node = self.body is None


    # The following are needed for networkx graphs
    def _key(self):
        return (self.name, self.dtype)

    def __hash__(self):
        return hash(self._key())

    def __eq__(self, other):
        if isinstance(other, tuple):
            return self._key() == other
        if isinstance(other, VariableNode):
            return self._key() == other._key()
        return False

    # Might be useful for visualization
    def __str__(self):
        return f'{self.name}{"" if self.is_initial_node else " *"}'


    # Purely for convenience
    def __call__(self, *args, **kwargs):
        return self.body(*args, **kwargs)
