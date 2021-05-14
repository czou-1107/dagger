from dataclasses import dataclass, field
from inspect import signature, Signature
from typing import Dict, Callable, List, Set, Union

import networkx as nx
import pandas as pd
from networkx.algorithms import dag

from .node import VariableNode


@dataclass
class ComputationGraph:
    """
    Computation engine: abstracts a set of computations on variables as a DAG

    A variable is a node, and it is either an initial node (no parents, assumed
    to exist in input data), or a computed node (depends on an initial or prior
    computed node)
    """
    graph: nx.DiGraph
    execution_plan: List[VariableNode] = field(init=False, default=None)
    initial_nodes: Set[str] = field(init=False, default=None)


    @classmethod
    def from_funcs(cls, funcs: Dict[str, Callable]):
        """
        Create a DAG of variables inferred from function signatures
        """
        graph = nx.DiGraph()

        for nm, f in funcs.items():
            sig = signature(f)
            func_node = VariableNode(name=nm,
                                    dtype=sig.return_annotation,
                                    body=f)
            graph.add_node(func_node)

            for pnm, param in sig.parameters.items():
                parent_node = VariableNode(name=pnm,
                                        dtype=param.annotation)
                # Note: comparison logic here is based on VariableNode.__eq__()
                # parent_node is instantiated with enough parameters for equality
                # check only! The only nodes added here should be initial nodes!
                if parent_node not in graph:
                    graph.add_node(parent_node)
                graph.add_edge(parent_node, func_node)

        # Validate graph:
        if not dag.is_directed_acyclic_graph(graph):
            raise ValueError('Functions do not form a proper DAG!')

        return cls(graph)


    def plan(self):
        """
        Determine execution plan for graph

        Compute topological sort for computed variables
        """
        sorted_graph = dag.topological_sort(self.graph)

        initial_nodes = set()
        execution_plan = []

        for node in sorted_graph:
            if node.is_initial_node:
                initial_nodes.add(node.name)
            else:
                execution_plan.append(node)

        self.initial_nodes = initial_nodes
        self.execution_plan = execution_plan
        return self


    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the planned computations, sequentially, to a DataFrame
        """
        if (missing_vars := self.initial_nodes - set(data)):
            raise ValueError('Data missing required variables!', missing_vars)

        for node in self.execution_plan:
            depends = self.graph.predecessors(node)
            input_values = {n.name: data[n.name] for n in depends}
            node_value = node(**input_values)
            # Alternately:
            # sig = signature(node.body)
            # inputs = {p: data[p] for p in sig.parameters}
            # bound = sig.bind(**inputs)
            # node_value = node.body(**bound.arguments)

            data[node.name] = node_value

        return data


    def visualize(self):
        """ Use networkx's default plotting """
        nx.draw(self.graph, with_labels=True)