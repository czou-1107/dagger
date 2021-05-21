import logging
from typing import Dict, Callable, Optional, Union

import networkx as nx
import pandas as pd
from networkx.algorithms import dag

from .utils.type_checker import check_type

from .node import VariableNode, FunctionSignatureTuple


logger = logging.getLogger(__name__)


class ComputationGraph:
    """
    A DAG representing a set of dependent variable transforms

    Nodes are variables (either initial or computed) and edges are dependencies

    Usage:
    ------
    cg = ComputationGraph()
    cg.initialize(funcs)
    cg.plan()
    cg.apply(data)
    """
    def __init__(self, allow_undeclared_vars: bool = True):
        self.allow_undeclared_vars = allow_undeclared_vars
        self._graph = nx.DiGraph()
        self._is_planned = False


    def _add_function_nodes(self, func: Callable):
        """ Add nodes inferred from function signature"""
        # func node will always be complete
        func_info = FunctionSignatureTuple.from_signature(func)
        self._add_or_update_node(func_info.node)
        for parent in func_info.parents:
            # parent nodes will always be incomplete
            self._add_or_update_node(parent)
            self._graph.add_edge(parent.name, func_info.node.name)


    def _add_or_update_node(self, node: VariableNode):
        """ Update or add a new node """
        if node.name not in self._graph:
            return self._graph.add_node(node.name, data=node)

        existing_node = self._graph.nodes[node.name]['data']
        existing_node.check_for_update(node)


    def add_functions(self, funcs: Dict[str, Callable]):
        """ Add nodes to graph as inferred from functions

        Can be called multiple times """
        if self._is_planned:
            raise ValueError('Computation graph has already been planned. '
                             'No further functions can be added.')

        for _, f in funcs.items():
            self._add_function_nodes(f)

        if not dag.is_directed_acyclic_graph(self._graph):
            raise ValueError('Functions do not form a proper DAG!')


    def plan(self):
        """ Plan execution order of functions, along with initial conditions to check
        """
        if self._is_planned:
            logger.info('Graph is already planned. Skipping...')
            return

        sorted_graph = dag.topological_sort(self._graph)

        initial_nodes = set()
        execution_plan = []

        for nm in sorted_graph:
            node = self._graph.nodes[nm]['data']
            if not node.is_complete:
                initial_nodes.add(node.name)
            else:
                execution_plan.append(node)

        self.initial_nodes = initial_nodes
        self.execution_plan = execution_plan
        self._is_planned = True
        return self


    def _validate_data(self, data):
        """ Validate that all data elements are correctly typed, if applicable """
        nodes = self._graph.nodes
        for nm in data:
            try:
                node = nodes[nm]['data']
            except KeyError as e:
                if self.allow_undeclared_vars:
                    pass
                else:
                    raise ValueError('Graph does not allow undeclared variable:', nm) from e
            check_type(data[nm], node.dtype)


    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """ Apply planned transformations to data """
        if not self._is_planned:
            raise ValueError('Must first run .plan()')
        if (missing := self.initial_nodes.difference(data)):
            raise ValueError(f'Data missing columns: {missing}')

        self._validate_data(data)
        for node in self.execution_plan:
            depends = self._graph.predecessors(node.name)
            input_values = {n: data[n] for n in depends}
            node_value = node(**input_values)
            data[node.name] = node_value
        return data


    def visualize(self):
        """ Visualize computation graph """
        nx.draw(self._graph, with_labels=True)
