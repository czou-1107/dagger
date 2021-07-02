import logging
from pathlib import Path
from typing import Dict, Callable, List, Union

import dask.dataframe as dd
import networkx as nx
import pandas as pd
from networkx.algorithms import dag
from networkx.readwrite.json_graph import node_link_data, node_link_graph

from .utils.importer import extract_module_functions
from .utils.type_checker import check_type

from .node import VariableNode, FunctionSignatureTuple

DEFAULT_DASK_CHUNKSIZE = 10000

logger = logging.getLogger(__name__)


class DagExecutor:
    """
    A DAG representing a set of dependent variable transforms

    Nodes are variables (either initial or computed) and edges are dependencies

    Usage:
    ------
    ex = DagExecutor()
    cg.add_function_scripts('path/to/script/')
    cg.plan()
    cg.apply(data)
    """
    def __init__(self,
                 use_dask: bool = False,
                 dask_chunksize: int = DEFAULT_DASK_CHUNKSIZE,
                 allow_undeclared_vars: bool = True):
        self.allow_undeclared_vars = allow_undeclared_vars
        self.use_dask = use_dask
        self.dask_chunksize = dask_chunksize
        self._graph = nx.DiGraph()
        self._is_planned = False


    def __getitem__(self, nm: str) -> VariableNode:
        """ Return node instance located at :nm: """
        return self._graph.nodes[nm]['data']


    def __contains__(self, nm: str) -> bool:
        return nm in self._graph.nodes


    def _add_or_update_node(self, node: VariableNode):
        """ Update or add a new node """
        if node.name not in self._graph:
            return self._graph.add_node(node.name, data=node)

        existing_node = self[node.name]
        existing_node.check_for_update(node)


    def _add_function_nodes(self, func: Callable):
        """ Add nodes inferred from function signature"""
        # func node will always be complete
        func_info = FunctionSignatureTuple.from_signature(func)
        self._add_or_update_node(func_info.node)
        for parent in func_info.parents:
            # parent nodes will always be incomplete
            self._add_or_update_node(parent)
            self._graph.add_edge(parent.name, func_info.node.name)


    def add_functions(self, funcs: Dict[str, Callable]):
        """ Add nodes to graph as inferred from functions

        Can be called multiple times. Chainable.
        Plan is generated and graph finalized using plan().
        """
        if self._is_planned:
            raise ValueError('Computation graph has already been planned. '
                             'No further functions can be added.')

        for _, f in funcs.items():
            self._add_function_nodes(f)

        if not dag.is_directed_acyclic_graph(self._graph):
            raise ValueError('Functions do not form a proper DAG!')
        return self


    def add_function_scripts(self, scripts: Union[List[Union[str, Path]], str, Path]):
        """ Add nodes to graph inferred from functions in transform scripts

        Can be called multiple times. Chainable.
        Plan is generated and graph finalized using plan() """
        if isinstance(scripts, (Path, str)):
            scripts = [scripts]

        for script in scripts:
            funcs = extract_module_functions(script)
            self.add_functions(funcs)
        return self


    def plan(self):
        """ Plan execution order of functions, along with initial conditions to check

        Chainable.
        """
        if len(self._graph) == 0:
            logger.warning('Planning an empty graph!')
        if self._is_planned:
            logger.info('Graph is already planned. Skipping...')
            return

        sorted_graph = dag.topological_sort(self._graph)

        initial_nodes = set()
        execution_plan = []

        for nm in sorted_graph:
            node = self[nm]
            if not node.is_complete:
                initial_nodes.add(node.name)
            else:
                execution_plan.append(node)

        self.initial_nodes = initial_nodes
        self.execution_plan = execution_plan
        self._graph = nx.freeze(self._graph)
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
                    continue
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
        if self.use_dask:
            data = dd.from_pandas(data, chunksize=self.dask_chunksize)

        for node in self.execution_plan:
            depends = self._graph.predecessors(node.name)
            input_values = {n: data[n] for n in depends}
            node_value = node(**input_values)
            data[node.name] = pd.Series(node_value)

        if self.use_dask:
            data = data.compute()
        return data


    def visualize(self):
        """ Visualize computation graph """
        nx.draw(self._graph, with_labels=True)
