"""
Main package class: imports script and applies transform functions to DataFrame
"""
from dataclasses import dataclass, field

import pandas as pd

from .utils.importer import extract_module_functions
from .graph import ComputationGraph
from .node import VariableNode


@dataclass
class DagExecutor:
    """ Wrapper around ComputationGraph """
    # Is this wrapper even necessary? Potential uses:
    # - Link this to CLI, including loading and saving data
    # - Have this deal with the script loading and parsing, including multiple scripts
    script: str
    graph: ComputationGraph = field(init=False, default=None)


    def plan(self):
        funcs = extract_module_functions(self.script)

        self.graph = ComputationGraph.from_funcs(funcs)
        self.graph.plan()

        return self


    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.graph.apply(data)


    def get_graph(self):
        return self.graph.graph