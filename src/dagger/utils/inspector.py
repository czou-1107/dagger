from dataclasses import dataclass, field
from inspect import signature, Signature
from typing import Callable, Dict, List, Optional, Union

import networkx as nx
from networkx.algorithms import dag

"""
A DAG will be represented by networkx.DiGraph
Nodes are variables. We may have several types of variables:
- Initial variables
- Computed variables
Edges represent dependencies
A topological sort can be performed, and at least a basic sequential executor
can be used to run through transforms

Nodes will be represented by VariableNode, which contain the following:
- Name
- Type: when instantiated, values should be this type
- Parents: if a computed node, this will be used to create edges
- Body: function body to instantiate variable from parents
"""
@dataclass
class VariableNode:
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


def graph_from_funcs(funcs: Dict[str, Callable]):
    variables = {}
    graph = nx.DiGraph()

    for nm, f in funcs.items():
        sig = signature(f)
        func_node = VariableNode(name=nm,
                                 dtype=sig.return_annotation,
                                 body=f)
        graph.add_node(func_node)

        for pnm, param in sig.parameters.items():
            parent_node = VariableNode(name=pnm,
                                       dtype=sig.return_annotation)
            if parent_node not in graph:
                graph.add_node(parent_node)
            graph.add_edge(parent_node, func_node)

    # Validate graph:
    if not dag.is_directed_acyclic_graph(graph):
        raise ValueError('Functions do not form a proper DAG!')

    return graph


def _get_graph_execution_plan(graph):
    sorted_graph = dag.topological_sort(graph)

    initial_variables = set()
    execution_plan = []

    for node in sorted_graph:
        if node.is_initial_node:
            initial_variables.add(node.name)
        else:
            execution_plan.append(node)

    return {
        'initial_variables': initial_variables,
        'execution_plan': execution_plan,
    }


def apply_graph(graph, data):
    plan = _get_graph_execution_plan(graph)
    available_vars = plan['initial_variables'].copy()
    assert available_vars <= set(data)

    for node in plan['execution_plan']:
        # Need to assert depends is subset of available_vars?
        depends = graph.predecessors(node)
        inputs = {n.name: data[n.name] for n in graph.predecessors(node)}
        node_value = node.body(**inputs)
        # Alternately:
        # sig = signature(node.body)
        # inputs = {p: data[p] for p in sig.parameters}
        # bound = sig.bind(**inputs)
        # node_value = node.body(**bound.arguments)

        data[node.name] = node_value
        available_vars.add(node.name)

    return data
