"""
Main package class: imports script and applies transform functions to DataFrame
"""
from pathlib import Path
from typing import List, Union

import pandas as pd

from .utils.importer import extract_module_functions
from .graph import ComputationGraph


class DagExecutor:
    """ Wrapper around ComputationGraph """
    # Is this wrapper even necessary? Potential uses:
    # - Link this to CLI, including loading and saving data
    # - Have this deal with the script loading and parsing, including multiple scripts
    def __init__(self, scripts: Union[List[Union[str, Path]], str, Path]):
        """ Parse transformation scripts to compute execution DAGs
        """
        if isinstance(scripts, (str, Path)):
            scripts = [scripts]
        self.scripts = scripts
        self._computation_graph = ComputationGraph()


    def plan(self):
        """ Load in modules and build execution DAG """
        for script in self.scripts:
            funcs = extract_module_functions(script)
            self._computation_graph.add_functions(funcs)

        self._computation_graph.plan()
        return self


    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """ Apply execution DAG to data """
        return self._computation_graph.apply(data)


    def visualize(self):
        self._computation_graph.visualize()


    def get_digraph(self):
        return self._computation_graph._graph
